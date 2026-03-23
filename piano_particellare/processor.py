# -*- coding: utf-8 -*-
"""Core processing logic for Piano Particellare."""

from __future__ import annotations

import math
from collections import defaultdict
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
    QgsPointXY,
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
    gruppo_field: str
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

    EMPTY_GROUP_LABEL = "SENZA_GRUPPO"
    EMPTY_RIGHT_LABEL = "SENZA_DIRITTO"
    OUTPUT_FIELDS = (
        ("uid", QVariant.Int, 0, 0),
        ("gruppo", QVariant.String, 120, 0),
        ("id_prog", QVariant.Int, 0, 0),
        ("id_part", QVariant.Int, 0, 0),
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
        self._blank_group_replacements = 0
        self._blank_right_replacements = 0

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

        self._emit_progress(90, "Generazione Excel...")
        excel_path = self._write_excel(output_layer)
        self.log.add(f"Output Excel scritto in: {excel_path}")

        log_path = ""
        if self.config.save_log:
            self._emit_progress(94, "Scrittura file di log...")
            log_path = self._write_log_file(excel_path)

        if self.config.add_to_project:
            self._emit_progress(97, "Caricamento layer nel progetto...")
            self._load_output_layer(written_layer_path)

        self._emit_progress(100, "Elaborazione completata.")
        return {
            "output_path": written_layer_path,
            "excel_path": excel_path,
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
            self._validate_field(layer, opere_cfg.gruppo_field, f"gruppo opere ({layer.name()})")
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
                f"gruppo={opere_cfg.gruppo_field}, diritto={opere_cfg.diritto_field}, "
                f"tipo={opere_cfg.tipo_opera_field}, id_opera={opere_cfg.id_opera_field or '[auto]'})"
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
            gruppo_idx = layer.fields().indexFromName(opere_cfg.gruppo_field)
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

                gruppo_value = self._normalize_group_value(opere_feature[gruppo_idx], layer.name(), opere_feature.id())
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

                    polygon_parts = self._extract_polygon_parts(intersection)
                    if not polygon_parts:
                        self.log.warning(
                            f"Intersezione senza parti poligonali utili: opera {opere_feature.id()} layer {layer.name()}, particella {cadastral_feature.id()}, wkb={QgsWkbTypes.displayString(intersection.wkbType())}."
                        )
                        continue

                    for part_geom in polygon_parts:
                        if part_geom.isEmpty() or part_geom.area() <= 0:
                            continue

                        new_feature = QgsFeature(output_layer.fields())
                        new_feature.setGeometry(part_geom)
                        new_feature["uid"] = uid
                        new_feature["gruppo"] = gruppo_value
                        new_feature["id_prog"] = None
                        new_feature["id_part"] = None
                        new_feature["comune"] = self._safe_string(cadastral_feature[comune_idx])
                        new_feature["foglio"] = self._safe_string(cadastral_feature[foglio_idx])
                        new_feature["particella"] = self._safe_string(cadastral_feature[particella_idx])
                        new_feature["diritto"] = diritto_value
                        new_feature["tipo_op"] = tipo_value
                        new_feature["id_opera"] = id_value
                        new_feature["src_layer"] = layer.name()
                        new_feature["area_mq"] = self.round_area_for_reporting(float(part_geom.area()))
                        output_features.append(new_feature)
                        uid += 1
                        self.log.created_features += 1

        if output_features:
            self._assign_id_part(output_features)
            self._assign_id_prog(output_features)
            provider.addFeatures(output_features)
            output_layer.updateExtents()
        self.log.add(f"Feature opere processate: {self.log.processed_opere}")
        self.log.add(f"Feature di output create: {self.log.created_features}")
        self.log.add(f"Feature saltate: {self.log.skipped_features}")
        return output_layer, len(output_features)

    def _assign_id_part(self, output_features: List[QgsFeature]) -> None:
        first_seen: Dict[Tuple[str, str, str], int] = {}
        parcels: Dict[Tuple[str, str, str], List[QgsFeature]] = defaultdict(list)

        for index, feature in enumerate(output_features):
            key = self._parcel_key(feature)
            parcels[key].append(feature)
            first_seen.setdefault(key, index)

        ordered_parcels = sorted(
            parcels.keys(),
            key=lambda key: (
                self._sortable_mixed_value(key[1]),
                self._sortable_mixed_value(key[2]),
                key[0].casefold(),
                key[1],
                key[2],
                first_seen[key],
            ),
        )

        self.log.add(f"Assegnazione id_part: {len(ordered_parcels)} particelle univoche ordinate.")
        for id_part, parcel_key in enumerate(ordered_parcels, start=1):
            for feature in parcels[parcel_key]:
                feature["id_part"] = id_part
        self.log.add(f"Numero particelle univoche con id_part: {len(ordered_parcels)}")

    def _assign_id_prog(self, output_features: List[QgsFeature]) -> None:
        indexed_features: List[Tuple[int, QgsFeature]] = list(enumerate(output_features))
        groups: Dict[str, List[Tuple[int, QgsFeature]]] = defaultdict(list)
        for index, feature in indexed_features:
            group_value = self._normalize_group_value(feature["gruppo"])
            feature["gruppo"] = group_value
            groups[group_value].append((index, feature))

        ordered_groups = sorted(groups.keys(), key=lambda value: value.casefold())
        self.log.add(f"Numero gruppi distinti trovati: {len(ordered_groups)}")
        self.log.add(f"Ordine gruppi elaborati: {', '.join(ordered_groups)}")

        next_id = 1
        for group_name in ordered_groups:
            ordered_indexes = self._order_group_feature_indexes(groups[group_name])
            self.log.add(
                f"Ordinamento id_prog per gruppo '{group_name}': {len(ordered_indexes)} porzioni assegnate da {next_id} a {next_id + len(ordered_indexes) - 1}."
            )
            for feature_index in ordered_indexes:
                output_features[feature_index]["id_prog"] = next_id
                next_id += 1

    def _order_group_feature_indexes(self, indexed_features: List[Tuple[int, QgsFeature]]) -> List[int]:
        feature_map = {index: feature for index, feature in indexed_features}
        remaining = {index for index, _ in indexed_features}
        ordered: List[int] = []

        start_index = min(remaining, key=lambda idx: self._centroid_sort_key(feature_map[idx], idx))
        block_indexes = self._consume_parcel_block(start_index, remaining, feature_map)
        ordered.extend(block_indexes)
        reference_index = start_index

        while remaining:
            next_index = min(
                remaining,
                key=lambda idx: self._distance_sort_key(feature_map[reference_index], feature_map[idx], idx),
            )
            block_indexes = self._consume_parcel_block(next_index, remaining, feature_map)
            ordered.extend(block_indexes)
            reference_index = next_index

        return ordered

    def _consume_parcel_block(
        self,
        start_index: int,
        remaining: set[int],
        feature_map: Dict[int, QgsFeature],
    ) -> List[int]:
        parcel = self._parcel_key(feature_map[start_index])
        parcel_remaining = {idx for idx in remaining if self._parcel_key(feature_map[idx]) == parcel}
        ordered = [start_index]
        remaining.remove(start_index)
        parcel_remaining.remove(start_index)
        current_index = start_index

        while parcel_remaining:
            next_index = min(
                parcel_remaining,
                key=lambda idx: self._distance_sort_key(feature_map[current_index], feature_map[idx], idx),
            )
            ordered.append(next_index)
            remaining.remove(next_index)
            parcel_remaining.remove(next_index)
            current_index = next_index

        return ordered

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

        self.log.warning(
            f"{feature_type} {feature_id} del layer {layer_name}: geometria non valida rilevata, tentativo di correzione con makeValid()."
        )

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
            self.log.add(
                f"{feature_type} {feature_id} del layer {layer_name}: makeValid() ha prodotto 1 parte poligonale valida."
            )
            return polygon_parts[0]

        self.log.add(
            f"{feature_type} {feature_id} del layer {layer_name}: makeValid() ha prodotto {len(polygon_parts)} parti poligonali, avvio merge."
        )
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

        parts: List[QgsGeometry] = []
        stack: List[QgsGeometry] = [QgsGeometry(geometry)]

        while stack:
            current = stack.pop()
            if current is None or current.isEmpty():
                continue

            try:
                geom_type = QgsWkbTypes.geometryType(current.wkbType())
            except Exception:
                continue

            if geom_type != QgsWkbTypes.PolygonGeometry:
                continue

            if current.isMultipart():
                try:
                    sub_geoms = current.asGeometryCollection()
                except Exception:
                    sub_geoms = []

                if sub_geoms:
                    for sub in sub_geoms:
                        if sub is not None and not sub.isEmpty():
                            stack.append(QgsGeometry(sub))
                else:
                    try:
                        if current.area() > 0:
                            parts.append(QgsGeometry(current))
                    except Exception:
                        continue
            else:
                try:
                    if current.area() > 0:
                        parts.append(QgsGeometry(current))
                except Exception:
                    continue

        return parts

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

    def _write_excel(self, output_layer: QgsVectorLayer) -> str:
        output_path = Path(self.config.output_path)
        excel_path = output_path.with_suffix(".xlsx")
        try:
            from openpyxl import Workbook
        except ImportError as exc:
            raise PianoParticellareError("La libreria openpyxl è necessaria per esportare il file Excel (.xlsx).") from exc

        workbook = Workbook()
        detail_sheet = workbook.active
        detail_sheet.title = "Dettaglio"
        summary_sheet = workbook.create_sheet("Riepilogo")

        features = list(output_layer.getFeatures())
        ordered_features = sorted(
            features,
            key=lambda feature: (
                self._safe_int(feature["id_prog"]),
                self._safe_int(feature["uid"]),
            ),
        )
        field_names = [field.name() for field in output_layer.fields()]
        detail_sheet.append(field_names)
        for feature in ordered_features:
            row = []
            for field_name in field_names:
                value = feature[field_name]
                if field_name == "area_mq":
                    value = self.round_area_for_reporting(value)
                row.append(value)
            detail_sheet.append(row)

        distinct_rights = sorted(
            {self._normalize_diritto_value(feature["diritto"], count_warning=False) for feature in features},
            key=lambda value: value.casefold(),
        )
        summary_headers = ["id_part", "comune", "foglio", "particella", *distinct_rights]
        summary_sheet.append(summary_headers)

        parcel_rows: Dict[Tuple[str, str, str], Dict[str, object]] = {}
        for feature in ordered_features:
            parcel = self._parcel_key(feature)
            row = parcel_rows.setdefault(
                parcel,
                {
                    "id_part": feature["id_part"],
                    "comune": feature["comune"],
                    "foglio": feature["foglio"],
                    "particella": feature["particella"],
                    **{right: 0 for right in distinct_rights},
                },
            )
            right_name = self._normalize_diritto_value(feature["diritto"])
            row[right_name] += self.round_area_for_reporting(feature["area_mq"])

        ordered_summary_rows = sorted(
            parcel_rows.values(),
            key=lambda row: (
                self._safe_int(row["id_part"]),
                self._sortable_mixed_value(row["foglio"]),
                self._sortable_mixed_value(row["particella"]),
                self._safe_string(row["comune"]).casefold(),
            ),
        )
        for row in ordered_summary_rows:
            summary_sheet.append([row[column] for column in summary_headers])

        workbook.save(excel_path)
        self.log.add(f"Numero colonne dinamiche diritto nel riepilogo: {len(distinct_rights)}")
        self.log.add("Workbook Excel generato con fogli: Dettaglio, Riepilogo")
        return str(excel_path)

    def _write_log_file(self, excel_path: str) -> str:
        output_path = Path(self.config.output_path)
        log_path = output_path.with_name(f"{output_path.stem}_log.txt")
        if self._blank_group_replacements:
            self.log.warning(
                f"Valori gruppo nulli/vuoti sostituiti con '{self.EMPTY_GROUP_LABEL}': {self._blank_group_replacements} occorrenze."
            )
        if self._blank_right_replacements:
            self.log.warning(
                f"Valori diritto nulli/vuoti sostituiti con '{self.EMPTY_RIGHT_LABEL}' nel riepilogo Excel: {self._blank_right_replacements} occorrenze."
            )
        self.log.add(f"Warnings: {self.log.warnings}")
        self.log.add(f"Errors: {self.log.errors}")
        self.log.add(f"Output file path: {self.config.output_path}")
        self.log.add(f"Excel file path: {excel_path}")
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

    def _normalize_group_value(self, value, layer_name: str = "", feature_id: Optional[int] = None) -> str:
        text = self._safe_string(value).strip()
        if text:
            return text
        self._blank_group_replacements += 1
        if layer_name:
            self.log.warning(
                f"Feature {feature_id} del layer {layer_name}: gruppo opere nullo/vuoto sostituito con '{self.EMPTY_GROUP_LABEL}'."
            )
        return self.EMPTY_GROUP_LABEL

    def _normalize_diritto_value(self, value, count_warning: bool = True) -> str:
        text = self._safe_string(value).strip()
        if text:
            return text
        if count_warning:
            self._blank_right_replacements += 1
        return self.EMPTY_RIGHT_LABEL

    def _parcel_key(self, feature: QgsFeature) -> Tuple[str, str, str]:
        return (
            self._safe_string(feature["comune"]).strip(),
            self._safe_string(feature["foglio"]).strip(),
            self._safe_string(feature["particella"]).strip(),
        )

    def _centroid_xy(self, feature: QgsFeature) -> Tuple[float, float]:
        geometry = feature.geometry()
        if geometry is None or geometry.isEmpty():
            return (float("inf"), float("inf"))

        centroid = geometry.centroid()
        point: Optional[QgsPointXY] = None
        if centroid and not centroid.isEmpty():
            try:
                centroid_point = centroid.asPoint()
                point = QgsPointXY(centroid_point.x(), centroid_point.y())
            except Exception:
                point = None
        if point is None:
            point_geom = geometry.pointOnSurface()
            if point_geom and not point_geom.isEmpty():
                try:
                    point_value = point_geom.asPoint()
                    point = QgsPointXY(point_value.x(), point_value.y())
                except Exception:
                    point = None
        if point is None:
            bbox = geometry.boundingBox()
            point = QgsPointXY(bbox.center().x(), bbox.center().y())
        return (point.x(), point.y())

    def _centroid_sort_key(self, feature: QgsFeature, index: int) -> Tuple[float, float, int]:
        x_value, y_value = self._centroid_xy(feature)
        return (x_value, y_value, index)

    def _distance_sort_key(
        self,
        reference_feature: QgsFeature,
        candidate_feature: QgsFeature,
        candidate_index: int,
    ) -> Tuple[float, float, float, int]:
        reference_x, reference_y = self._centroid_xy(reference_feature)
        candidate_x, candidate_y = self._centroid_xy(candidate_feature)
        distance = math.hypot(candidate_x - reference_x, candidate_y - reference_y)
        return (distance, candidate_x, candidate_y, candidate_index)

    @staticmethod
    def _sortable_mixed_value(value) -> Tuple[int, float, str, str]:
        text = PianoParticellareProcessor._safe_string(value).strip()
        try:
            numeric_value = float(text.replace(",", "."))
            return (0, numeric_value, text.casefold(), text)
        except ValueError:
            return (1, 0.0, text.casefold(), text)

    @staticmethod
    def _safe_int(value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def round_area_for_reporting(value: float) -> int:
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return 0
        if numeric_value <= 0:
            return 0
        return int(math.ceil(numeric_value))

    @staticmethod
    def _safe_string(value) -> str:
        if value is None:
            return ""
        return str(value)
