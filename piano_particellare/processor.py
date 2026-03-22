# -*- coding: utf-8 -*-
"""Core processing logic for Piano Particellare."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from qgis.PyQt.QtCore import QVariant
from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsFeature,
    QgsField,
    QgsFields,
    QgsGeometry,
    QgsProject,
    QgsSpatialIndex,
    QgsVectorFileWriter,
    QgsVectorLayer,
    QgsWkbTypes,
)

ProgressCallback = Optional[Callable[[int, str], None]]
MessageCallback = Optional[Callable[[str], None]]


class PianoParticellareError(Exception):
    """Raised when the plugin input validation or processing fails."""


@dataclass
class OpereLayerConfig:
    """Configuration describing one opere layer and its field mapping."""

    layer: QgsVectorLayer
    diritto_field: str
    tipo_opera_field: str
    id_opera_field: str = ""


@dataclass
class ProcessorConfig:
    """Runtime configuration for the processing job."""

    cadastral_layer: QgsVectorLayer
    comune_field: str
    foglio_field: str
    particella_field: str
    opere_layers: Sequence[OpereLayerConfig]
    fix_geometries: bool
    add_to_project: bool
    save_log: bool
    output_path: str
    output_format: str
    output_layer_name: str = "piano_particellare"


@dataclass
class ProcessLog:
    """Collects structured log lines and counters."""

    lines: List[str] = field(default_factory=list)
    skipped_features: int = 0
    processed_opere: int = 0
    created_features: int = 0
    warnings: int = 0
    errors: int = 0

    def add(self, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.lines.append(f"[{timestamp}] {message}")

    def warning(self, message: str) -> None:
        self.warnings += 1
        self.add(f"WARNING: {message}")

    def error(self, message: str) -> None:
        self.errors += 1
        self.add(f"ERROR: {message}")

    def skipped(self, message: str) -> None:
        self.skipped_features += 1
        self.warning(f"Skipped feature: {message}")


class PianoParticellareProcessor:
    """Runs the parcel plan generation workflow."""

    OUTPUT_FIELDS = (
        ("uid", QVariant.Int, 0, 0),
        ("comune", QVariant.String, 120, 0),
        ("foglio", QVariant.String, 80, 0),
        ("particella", QVariant.String, 80, 0),
        ("diritto", QVariant.String, 120, 0),
        ("tipo_op", QVariant.String, 120, 0),
        ("id_opera", QVariant.String, 120, 0),
        ("src_layer", QVariant.String, 120, 0),
        ("area_mq", QVariant.Double, 20, 6),
    )

    def __init__(self, config: ProcessorConfig, progress: ProgressCallback = None, message: MessageCallback = None):
        self.config = config
        self.progress = progress
        self.message = message
        self.log = ProcessLog()

    def run(self) -> Dict[str, str]:
        """Execute the complete workflow and return output metadata."""
        self._emit_progress(0, "Validazione input...")
        self._validate_inputs()
        self._log_inputs()

        self._emit_progress(10, "Preparazione particelle catastali...")
        cadastral_features, spatial_index = self._prepare_cadastral_features()
        if not cadastral_features:
            raise PianoParticellareError("Nessuna geometria catastale valida disponibile dopo la validazione.")

        self._emit_progress(25, "Elaborazione intersezioni...")
        output_layer, output_count = self._build_output_layer(cadastral_features, spatial_index)

        if output_count == 0:
            raise PianoParticellareError("Nessuna intersezione poligonale generata. Verificare i layer di input.")

        self._emit_progress(85, "Scrittura layer di output...")
        written_layer_path = self._write_output(output_layer)
        self.log.add(f"Output scritto in: {written_layer_path}")

        log_path = ""
        if self.config.save_log:
            self._emit_progress(92, "Scrittura file di log...")
            log_path = self._write_log_file()

        if self.config.add_to_project:
            self._emit_progress(96, "Caricamento layer nel progetto...")
            self._load_output_layer(written_layer_path)

        self._emit_progress(100, "Elaborazione completata.")
        return {
            "output_path": written_layer_path,
            "log_path": log_path,
            "created_features": str(self.log.created_features),
            "skipped_features": str(self.log.skipped_features),
        }

    def _validate_inputs(self) -> None:
        cfg = self.config
        if not cfg.cadastral_layer or not cfg.cadastral_layer.isValid():
            raise PianoParticellareError("Selezionare un layer catastale valido.")
        if not cfg.output_path:
            raise PianoParticellareError("Specificare il percorso di output.")
        if cfg.output_format not in {"SHP", "GPKG"}:
            raise PianoParticellareError("Formato di output non supportato. Usare SHP o GPKG.")
        if not cfg.opere_layers:
            raise PianoParticellareError("Selezionare almeno un layer opere.")

        self._validate_polygon_layer(cfg.cadastral_layer, "catastale")
        self._validate_field(cfg.cadastral_layer, cfg.comune_field, "Comune")
        self._validate_field(cfg.cadastral_layer, cfg.foglio_field, "Foglio")
        self._validate_field(cfg.cadastral_layer, cfg.particella_field, "Particella")

        reference_crs = cfg.cadastral_layer.crs()
        if not reference_crs.isValid():
            raise PianoParticellareError("Il CRS del layer catastale non è valido.")

        for opere_cfg in cfg.opere_layers:
            layer = opere_cfg.layer
            if not layer or not layer.isValid():
                raise PianoParticellareError("Uno dei layer opere selezionati non è valido.")
            self._validate_polygon_layer(layer, f"opere ({layer.name()})")
            self._validate_field(layer, opere_cfg.diritto_field, f"diritto/servitù ({layer.name()})")
            self._validate_field(layer, opere_cfg.tipo_opera_field, f"tipo opera ({layer.name()})")
            if opere_cfg.id_opera_field:
                self._validate_field(layer, opere_cfg.id_opera_field, f"id_opera ({layer.name()})")
            if layer.crs() != reference_crs:
                raise PianoParticellareError(
                    "Tutti i layer di input devono avere lo stesso CRS. "
                    f"'{layer.name()}' usa {layer.crs().authid()} invece di {reference_crs.authid()}."
                )

        expected_suffix = ".shp" if cfg.output_format == "SHP" else ".gpkg"
        if not cfg.output_path.lower().endswith(expected_suffix):
            raise PianoParticellareError(
                f"L'estensione del file di output deve essere '{expected_suffix}' per il formato selezionato."
            )

    def _validate_polygon_layer(self, layer: QgsVectorLayer, label: str) -> None:
        if QgsWkbTypes.geometryType(layer.wkbType()) != QgsWkbTypes.PolygonGeometry:
            raise PianoParticellareError(f"Il layer {label} deve essere di tipo poligonale.")

    def _validate_field(self, layer: QgsVectorLayer, field_name: str, label: str) -> None:
        if not field_name:
            raise PianoParticellareError(f"Selezionare il campo '{label}'.")
        if layer.fields().indexFromName(field_name) < 0:
            raise PianoParticellareError(f"Il campo '{field_name}' non esiste nel layer {layer.name()}.")

    def _log_inputs(self) -> None:
        cfg = self.config
        crs_desc = cfg.cadastral_layer.crs().authid() or cfg.cadastral_layer.crs().description()
        self.log.add("Avvio elaborazione Piano Particellare")
        self.log.add(f"Layer catastale: {cfg.cadastral_layer.name()}")
        self.log.add(
            f"Campi catastali -> comune: {cfg.comune_field}, foglio: {cfg.foglio_field}, particella: {cfg.particella_field}"
        )
        self.log.add(f"CRS condiviso: {crs_desc}")
        self.log.add(f"Numero particelle catastali: {cfg.cadastral_layer.featureCount()}")
        self.log.add(f"Numero layer opere: {len(cfg.opere_layers)}")
        for opere_cfg in cfg.opere_layers:
            self.log.add(
                "Layer opere: "
                f"{opere_cfg.layer.name()} (features={opere_cfg.layer.featureCount()}, "
                f"diritto={opere_cfg.diritto_field}, tipo={opere_cfg.tipo_opera_field}, "
                f"id_opera={opere_cfg.id_opera_field or '[auto]'})"
            )
        self.log.add(f"Fix invalid geometries: {'SI' if cfg.fix_geometries else 'NO'}")
        self.log.add(f"Output richiesto: {cfg.output_path} ({cfg.output_format})")

    def _prepare_cadastral_features(self) -> Tuple[Dict[int, QgsFeature], QgsSpatialIndex]:
        processed: Dict[int, QgsFeature] = {}
        index = QgsSpatialIndex()

        for feature in self.config.cadastral_layer.getFeatures():
            geometry = feature.geometry()
            valid_geometry = self._validated_geometry(
                geometry,
                feature.id(),
                self.config.cadastral_layer.name(),
                feature_type="catastale",
            )
            if valid_geometry is None or valid_geometry.isEmpty():
                continue

            copy_feature = QgsFeature(feature)
            copy_feature.setGeometry(valid_geometry)
            processed[copy_feature.id()] = copy_feature
            index.addFeature(copy_feature)

        self.log.add(f"Particelle catastali valide indicizzate: {len(processed)}")
        return processed, index

    def _build_output_layer(
        self,
        cadastral_features: Dict[int, QgsFeature],
        spatial_index: QgsSpatialIndex,
    ) -> Tuple[QgsVectorLayer, int]:
        crs: QgsCoordinateReferenceSystem = self.config.cadastral_layer.crs()
        output_layer = QgsVectorLayer(f"Polygon?crs={crs.authid()}", self.config.output_layer_name, "memory")
        provider = output_layer.dataProvider()
        provider.addAttributes(self._output_fields())
        output_layer.updateFields()

        total_opere = sum(max(layer_cfg.layer.featureCount(), 0) for layer_cfg in self.config.opere_layers) or 1
        processed_so_far = 0
        uid = 1
        output_features: List[QgsFeature] = []

        comune_idx = self.config.cadastral_layer.fields().indexFromName(self.config.comune_field)
        foglio_idx = self.config.cadastral_layer.fields().indexFromName(self.config.foglio_field)
        particella_idx = self.config.cadastral_layer.fields().indexFromName(self.config.particella_field)

        for opere_cfg in self.config.opere_layers:
            layer = opere_cfg.layer
            diritto_idx = layer.fields().indexFromName(opere_cfg.diritto_field)
            tipo_idx = layer.fields().indexFromName(opere_cfg.tipo_opera_field)
            id_idx = layer.fields().indexFromName(opere_cfg.id_opera_field) if opere_cfg.id_opera_field else -1

            for opere_feature in layer.getFeatures():
                processed_so_far += 1
                self.log.processed_opere += 1
                self._emit_progress(
                    25 + int((processed_so_far / total_opere) * 55),
                    f"Elaborazione feature {processed_so_far}/{total_opere} del layer {layer.name()}...",
                )

                opere_geom = self._validated_geometry(opere_feature.geometry(), opere_feature.id(), layer.name(), "opere")
                if opere_geom is None or opere_geom.isEmpty():
                    continue

                candidate_ids = spatial_index.intersects(opere_geom.boundingBox())
                if not candidate_ids:
                    self.log.warning(
                        f"La feature {opere_feature.id()} del layer {layer.name()} non interseca alcuna particella catastale."
                    )
                    continue

                diritto_value = self._safe_string(opere_feature[diritto_idx])
                tipo_value = self._safe_string(opere_feature[tipo_idx])
                id_value = (
                    self._safe_string(opere_feature[id_idx]) if id_idx >= 0 and opere_feature[id_idx] not in [None, ""]
                    else f"{layer.name()}_{opere_feature.id()}"
                )

                for cadastral_id in candidate_ids:
                    cadastral_feature = cadastral_features.get(cadastral_id)
                    if cadastral_feature is None:
                        continue
                    cadastral_geom = cadastral_feature.geometry()
                    if not cadastral_geom.intersects(opere_geom):
                        continue

                    intersection = opere_geom.intersection(cadastral_geom)
                    if intersection.isEmpty():
                        continue

                    for part_geom in self._extract_polygon_parts(intersection):
                        if part_geom.isEmpty() or part_geom.area() <= 0:
                            continue

                        new_feature = QgsFeature(output_layer.fields())
                        new_feature.setGeometry(part_geom)
                        new_feature["uid"] = uid
                        new_feature["comune"] = self._safe_string(cadastral_feature[comune_idx])
                        new_feature["foglio"] = self._safe_string(cadastral_feature[foglio_idx])
                        new_feature["particella"] = self._safe_string(cadastral_feature[particella_idx])
                        new_feature["diritto"] = diritto_value
                        new_feature["tipo_op"] = tipo_value
                        new_feature["id_opera"] = id_value
                        new_feature["src_layer"] = layer.name()
                        new_feature["area_mq"] = float(part_geom.area())
                        output_features.append(new_feature)
                        uid += 1
                        self.log.created_features += 1

        if output_features:
            provider.addFeatures(output_features)
            output_layer.updateExtents()
        self.log.add(f"Feature opere processate: {self.log.processed_opere}")
        self.log.add(f"Feature di output create: {self.log.created_features}")
        self.log.add(f"Feature saltate: {self.log.skipped_features}")
        return output_layer, len(output_features)

    def _validated_geometry(
        self,
        geometry: QgsGeometry,
        feature_id: int,
        layer_name: str,
        feature_type: str,
    ) -> Optional[QgsGeometry]:
        if geometry is None or geometry.isEmpty():
            self.log.skipped(f"{feature_type} {feature_id} del layer {layer_name}: geometria vuota.")
            return None

        working_geometry = QgsGeometry(geometry)
        if working_geometry.isGeosValid():
            return working_geometry

        if not self.config.fix_geometries:
            self.log.skipped(
                f"{feature_type} {feature_id} del layer {layer_name}: geometria non valida e correzione disabilitata."
            )
            return None

        fixed_geometry = working_geometry.makeValid()
        if fixed_geometry.isEmpty() or not fixed_geometry.isGeosValid():
            self.log.skipped(
                f"{feature_type} {feature_id} del layer {layer_name}: geometria non correggibile tramite makeValid()."
            )
            return None

        polygon_parts = self._extract_polygon_parts(fixed_geometry)
        if not polygon_parts:
            self.log.skipped(
                f"{feature_type} {feature_id} del layer {layer_name}: makeValid() non ha restituito geometrie poligonali."
            )
            return None

        if len(polygon_parts) == 1:
            return polygon_parts[0]

        merged = QgsGeometry.unaryUnion(polygon_parts)
        if merged.isEmpty() or not merged.isGeosValid():
            self.log.skipped(
                f"{feature_type} {feature_id} del layer {layer_name}: impossibile ricostruire una geometria valida dopo makeValid()."
            )
            return None
        return merged

    def _extract_polygon_parts(self, geometry: QgsGeometry) -> List[QgsGeometry]:
        if geometry is None or geometry.isEmpty():
            return []

        result: List[QgsGeometry] = []
        collection = geometry.asGeometryCollection()
        if collection:
            for item in collection:
                result.extend(self._extract_polygon_parts(item))
            return result

        if QgsWkbTypes.geometryType(geometry.wkbType()) != QgsWkbTypes.PolygonGeometry:
            return []

        if geometry.isMultipart():
            for item in geometry.asGeometryCollection():
                if QgsWkbTypes.geometryType(item.wkbType()) == QgsWkbTypes.PolygonGeometry and item.area() > 0:
                    result.append(item)
            return result

        return [QgsGeometry(geometry)]

    def _write_output(self, output_layer: QgsVectorLayer) -> str:
        save_options = QgsVectorFileWriter.SaveVectorOptions()
        save_options.driverName = "ESRI Shapefile" if self.config.output_format == "SHP" else "GPKG"
        save_options.fileEncoding = "UTF-8"
        save_options.layerName = Path(self.config.output_path).stem

        transform_context = QgsProject.instance().transformContext()
        error_code, error_message, _, _ = QgsVectorFileWriter.writeAsVectorFormatV3(
            output_layer,
            self.config.output_path,
            transform_context,
            save_options,
        )
        if error_code != QgsVectorFileWriter.NoError:
            raise PianoParticellareError(f"Errore nella scrittura del layer di output: {error_message}")
        return self.config.output_path

    def _write_log_file(self) -> str:
        output_path = Path(self.config.output_path)
        log_path = output_path.with_name(f"{output_path.stem}_log.txt")
        self.log.add(f"Warnings: {self.log.warnings}")
        self.log.add(f"Errors: {self.log.errors}")
        self.log.add(f"Output file path: {self.config.output_path}")
        log_path.write_text("\n".join(self.log.lines), encoding="utf-8")
        return str(log_path)

    def _load_output_layer(self, output_path: str) -> None:
        layer_name = Path(output_path).stem
        output_layer = QgsVectorLayer(output_path, layer_name, "ogr")
        if not output_layer.isValid():
            self.log.warning("Il layer di output è stato scritto ma non è stato possibile caricarlo nel progetto.")
            return
        QgsProject.instance().addMapLayer(output_layer)

    def _output_fields(self) -> QgsFields:
        fields = QgsFields()
        for name, variant_type, length, precision in self.OUTPUT_FIELDS:
            field = QgsField(name, variant_type)
            if length:
                field.setLength(length)
            if precision:
                field.setPrecision(precision)
            fields.append(field)
        return fields

    def _emit_progress(self, value: int, message: str) -> None:
        if self.progress:
            self.progress(value, message)
        if self.message:
            self.message(message)

    @staticmethod
    def _safe_string(value) -> str:
        if value is None:
            return ""
        return str(value)
