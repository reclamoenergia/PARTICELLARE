# -*- coding: utf-8 -*-
"""Main dialog for configuring and running Piano Particellare."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)
from qgis.core import Qgis, QgsMapLayerType, QgsProject, QgsVectorLayer, QgsWkbTypes

from .processor import (
    OpereLayerConfig,
    PianoParticellareError,
    PianoParticellareProcessor,
    ProcessorConfig,
)


class PianoParticellareDialog(QDialog):
    """Dialog used to configure plugin inputs and run processing."""

    OPERE_LAYER_ROLE = Qt.UserRole + 1

    def __init__(self, iface, parent: Optional[QWidget] = None):
        super().__init__(parent or iface.mainWindow())
        self.iface = iface
        self.setWindowTitle("Piano Particellare")
        self.resize(1120, 760)

        self._build_ui()
        self._connect_signals()
        self.refresh_layers()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        layout.addWidget(self._build_cadastral_section())
        layout.addWidget(self._build_opere_section())
        layout.addWidget(self._build_options_section())
        layout.addWidget(self._build_output_section())
        layout.addWidget(self._build_run_section())

    def _build_cadastral_section(self) -> QGroupBox:
        box = QGroupBox("Sezione 1 - Layer catastale")
        form = QFormLayout(box)

        self.cadastral_layer_combo = QComboBox()
        self.comune_field_combo = QComboBox()
        self.foglio_field_combo = QComboBox()
        self.particella_field_combo = QComboBox()

        form.addRow("Layer catastale", self.cadastral_layer_combo)
        form.addRow("Campo Comune", self.comune_field_combo)
        form.addRow("Campo Foglio", self.foglio_field_combo)
        form.addRow("Campo Particella", self.particella_field_combo)
        return box

    def _build_opere_section(self) -> QGroupBox:
        box = QGroupBox("Sezione 2 - Layer opere")
        layout = QVBoxLayout(box)

        buttons_layout = QHBoxLayout()
        self.add_opere_button = QPushButton("Aggiungi layer")
        self.remove_opere_button = QPushButton("Rimuovi selezionato")
        buttons_layout.addWidget(self.add_opere_button)
        buttons_layout.addWidget(self.remove_opere_button)
        buttons_layout.addStretch(1)

        self.opere_table = QTableWidget(0, 5)
        self.opere_table.setHorizontalHeaderLabels(
            ["Layer", "Gruppo opere", "Diritto/Servitù", "Tipo opera", "ID opera (opz.)"]
        )
        header = self.opere_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        for column in (1, 2, 3, 4):
            header.setSectionResizeMode(column, QHeaderView.Stretch)
        self.opere_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.opere_table.setSelectionMode(QTableWidget.SingleSelection)
        self.opere_table.verticalHeader().setVisible(False)

        layout.addLayout(buttons_layout)
        layout.addWidget(self.opere_table)
        return box

    def _build_options_section(self) -> QGroupBox:
        box = QGroupBox("Sezione 3 - Opzioni")
        layout = QHBoxLayout(box)

        self.fix_geometries_checkbox = QCheckBox("Fix invalid geometries")
        self.fix_geometries_checkbox.setChecked(True)
        self.add_to_project_checkbox = QCheckBox("Aggiungi output al progetto")
        self.add_to_project_checkbox.setChecked(True)
        self.save_log_checkbox = QCheckBox("Salva file di log")
        self.save_log_checkbox.setChecked(True)

        layout.addWidget(self.fix_geometries_checkbox)
        layout.addWidget(self.add_to_project_checkbox)
        layout.addWidget(self.save_log_checkbox)
        layout.addStretch(1)
        return box

    def _build_output_section(self) -> QGroupBox:
        box = QGroupBox("Sezione 4 - Output")
        grid = QGridLayout(box)

        self.output_path_edit = QLineEdit()
        self.output_browse_button = QPushButton("Sfoglia...")
        self.output_format_combo = QComboBox()
        self.output_format_combo.addItems(["SHP", "GPKG"])

        grid.addWidget(QLabel("Percorso file"), 0, 0)
        grid.addWidget(self.output_path_edit, 0, 1)
        grid.addWidget(self.output_browse_button, 0, 2)
        grid.addWidget(QLabel("Formato"), 1, 0)
        grid.addWidget(self.output_format_combo, 1, 1)
        return box

    def _build_run_section(self) -> QGroupBox:
        box = QGroupBox("Sezione 5 - Esecuzione")
        layout = QVBoxLayout(box)

        self.run_button = QPushButton("Run")
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.messages_edit = QPlainTextEdit()
        self.messages_edit.setReadOnly(True)
        self.messages_edit.setPlaceholderText("Messaggi di avanzamento, warning ed errori...")

        layout.addWidget(self.run_button)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.messages_edit)
        return box

    def _connect_signals(self) -> None:
        self.cadastral_layer_combo.currentIndexChanged.connect(self._refresh_cadastral_fields)
        self.add_opere_button.clicked.connect(self._add_opere_layer)
        self.remove_opere_button.clicked.connect(self._remove_selected_opere_layer)
        self.output_browse_button.clicked.connect(self._browse_output_path)
        self.output_format_combo.currentIndexChanged.connect(self._update_output_extension)
        self.run_button.clicked.connect(self._run_processing)

    def refresh_layers(self) -> None:
        current_layer_id = self.cadastral_layer_combo.currentData()
        polygon_layers = self._available_polygon_layers()

        self.cadastral_layer_combo.blockSignals(True)
        self.cadastral_layer_combo.clear()
        for layer in polygon_layers:
            self.cadastral_layer_combo.addItem(layer.name(), layer.id())
        if current_layer_id:
            idx = self.cadastral_layer_combo.findData(current_layer_id)
            if idx >= 0:
                self.cadastral_layer_combo.setCurrentIndex(idx)
        self.cadastral_layer_combo.blockSignals(False)
        self._refresh_cadastral_fields()

        valid_layer_ids = {layer.id() for layer in polygon_layers}
        for row in reversed(range(self.opere_table.rowCount())):
            layer = self.opere_table.item(row, 0).data(self.OPERE_LAYER_ROLE)
            if layer is None or layer.id() not in valid_layer_ids:
                self.opere_table.removeRow(row)
                continue
            self._refresh_row_field_combos(row, layer)

    def clear_messages(self) -> None:
        self.progress_bar.setValue(0)
        self.messages_edit.clear()

    def _available_polygon_layers(self) -> List[QgsVectorLayer]:
        layers: List[QgsVectorLayer] = []
        for layer in QgsProject.instance().mapLayers().values():
            if layer.type() != QgsMapLayerType.VectorLayer:
                continue
            if QgsWkbTypes.geometryType(layer.wkbType()) != QgsWkbTypes.PolygonGeometry:
                continue
            layers.append(layer)
        layers.sort(key=lambda item: item.name().lower())
        return layers

    def _refresh_cadastral_fields(self) -> None:
        layer = self._selected_cadastral_layer()
        self._populate_field_combo(self.comune_field_combo, layer)
        self._populate_field_combo(self.foglio_field_combo, layer)
        self._populate_field_combo(self.particella_field_combo, layer)

    def _selected_cadastral_layer(self) -> Optional[QgsVectorLayer]:
        layer_id = self.cadastral_layer_combo.currentData()
        if not layer_id:
            return None
        return QgsProject.instance().mapLayer(layer_id)

    def _populate_field_combo(self, combo: QComboBox, layer: Optional[QgsVectorLayer], allow_empty: bool = False) -> None:
        current = combo.currentText()
        combo.clear()
        if allow_empty:
            combo.addItem("", "")
        if not layer:
            return
        for field in layer.fields():
            combo.addItem(field.name(), field.name())
        idx = combo.findData(current)
        if idx >= 0:
            combo.setCurrentIndex(idx)

    def _add_opere_layer(self) -> None:
        polygon_layers = self._available_polygon_layers()
        used_layer_ids = {
            self.opere_table.item(row, 0).data(self.OPERE_LAYER_ROLE).id()
            for row in range(self.opere_table.rowCount())
            if self.opere_table.item(row, 0) and self.opere_table.item(row, 0).data(self.OPERE_LAYER_ROLE)
        }
        candidates = [layer for layer in polygon_layers if layer.id() not in used_layer_ids]
        if not candidates:
            QMessageBox.information(self, "Piano Particellare", "Non ci sono altri layer poligonali disponibili da aggiungere.")
            return

        labels = [layer.name() for layer in candidates]
        selected_label, ok = QInputDialog.getItem(self, "Seleziona layer opere", "Layer opere", labels, 0, False)
        if not ok or not selected_label:
            return

        selected_layer = next(layer for layer in candidates if layer.name() == selected_label)
        row = self.opere_table.rowCount()
        self.opere_table.insertRow(row)

        item = QTableWidgetItem(selected_layer.name())
        item.setFlags(item.flags() & ~Qt.ItemIsEditable)
        item.setData(self.OPERE_LAYER_ROLE, selected_layer)
        self.opere_table.setItem(row, 0, item)

        for column in (1, 2, 3, 4):
            combo = QComboBox()
            self.opere_table.setCellWidget(row, column, combo)

        self._refresh_row_field_combos(row, selected_layer)

    def _refresh_row_field_combos(self, row: int, layer: QgsVectorLayer) -> None:
        for column, allow_empty in ((1, False), (2, False), (3, False), (4, True)):
            combo = self.opere_table.cellWidget(row, column)
            if isinstance(combo, QComboBox):
                self._populate_field_combo(combo, layer, allow_empty=allow_empty)

    def _remove_selected_opere_layer(self) -> None:
        selected_rows = self.opere_table.selectionModel().selectedRows()
        if not selected_rows:
            return
        self.opere_table.removeRow(selected_rows[0].row())

    def _browse_output_path(self) -> None:
        output_format = self.output_format_combo.currentText()
        file_filter = "ESRI Shapefile (*.shp)" if output_format == "SHP" else "GeoPackage (*.gpkg)"
        current_path = self.output_path_edit.text().strip() or str(Path.home())
        path, _ = QFileDialog.getSaveFileName(self, "Seleziona output", current_path, file_filter)
        if path:
            self.output_path_edit.setText(path)
            self._update_output_extension()

    def _update_output_extension(self) -> None:
        path = self.output_path_edit.text().strip()
        if not path:
            return
        selected_suffix = ".shp" if self.output_format_combo.currentText() == "SHP" else ".gpkg"
        stem = str(Path(path).with_suffix(""))
        self.output_path_edit.setText(f"{stem}{selected_suffix}")

    def _collect_opere_configs(self) -> List[OpereLayerConfig]:
        configs: List[OpereLayerConfig] = []
        for row in range(self.opere_table.rowCount()):
            layer_item = self.opere_table.item(row, 0)
            layer = layer_item.data(self.OPERE_LAYER_ROLE) if layer_item else None
            if not layer:
                continue
            gruppo_combo = self.opere_table.cellWidget(row, 1)
            diritto_combo = self.opere_table.cellWidget(row, 2)
            tipo_combo = self.opere_table.cellWidget(row, 3)
            id_combo = self.opere_table.cellWidget(row, 4)
            configs.append(
                OpereLayerConfig(
                    layer=layer,
                    gruppo_field=gruppo_combo.currentData() if isinstance(gruppo_combo, QComboBox) else "",
                    diritto_field=diritto_combo.currentData() if isinstance(diritto_combo, QComboBox) else "",
                    tipo_opera_field=tipo_combo.currentData() if isinstance(tipo_combo, QComboBox) else "",
                    id_opera_field=id_combo.currentData() if isinstance(id_combo, QComboBox) else "",
                )
            )
        return configs

    def _run_processing(self) -> None:
        self.clear_messages()
        cadastral_layer = self._selected_cadastral_layer()
        config = ProcessorConfig(
            cadastral_layer=cadastral_layer,
            comune_field=self.comune_field_combo.currentData() or "",
            foglio_field=self.foglio_field_combo.currentData() or "",
            particella_field=self.particella_field_combo.currentData() or "",
            opere_layers=self._collect_opere_configs(),
            fix_geometries=self.fix_geometries_checkbox.isChecked(),
            add_to_project=self.add_to_project_checkbox.isChecked(),
            save_log=self.save_log_checkbox.isChecked(),
            output_path=self.output_path_edit.text().strip(),
            output_format=self.output_format_combo.currentText(),
            output_layer_name=Path(self.output_path_edit.text().strip() or "piano_particellare").stem or "piano_particellare",
        )

        self.run_button.setEnabled(False)
        try:
            processor = PianoParticellareProcessor(
                config=config,
                progress=self._handle_progress,
            )
            result = processor.run()
            self.iface.messageBar().pushMessage(
                "Piano Particellare",
                f"Elaborazione completata. Feature create: {result['created_features']}.",
                level=Qgis.Success,
                duration=6,
            )
            final_message = f"Output creato: {result['output_path']}\nExcel creato: {result['excel_path']}"
            if result.get("log_path"):
                final_message += f"\nLog salvato in: {result['log_path']}"
            QMessageBox.information(self, "Piano Particellare", final_message)
        except PianoParticellareError as exc:
            self._append_message(f"ERRORE: {exc}")
            self.iface.messageBar().pushMessage("Piano Particellare", str(exc), level=Qgis.Critical, duration=8)
            QMessageBox.critical(self, "Piano Particellare", str(exc))
        except Exception as exc:  # pylint: disable=broad-except
            self._append_message(f"ERRORE inatteso: {exc}")
            self.iface.messageBar().pushMessage(
                "Piano Particellare",
                f"Errore inatteso: {exc}",
                level=Qgis.Critical,
                duration=8,
            )
            QMessageBox.critical(self, "Piano Particellare", f"Errore inatteso:\n{exc}")
        finally:
            self.run_button.setEnabled(True)

    def _handle_progress(self, value: int, message: str) -> None:
        self.progress_bar.setValue(value)
        self._append_message(message)

    def _append_message(self, message: str) -> None:
        self.messages_edit.appendPlainText(message)
