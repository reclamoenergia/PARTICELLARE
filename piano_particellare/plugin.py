# -*- coding: utf-8 -*-
"""Main plugin class for Piano Particellare."""

from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction
from qgis.core import Qgis

from .dialog import PianoParticellareDialog


class PianoParticellarePlugin:
    """QGIS plugin bootstrap class."""

    def __init__(self, iface):
        self.iface = iface
        self.action = None
        self.dialog = None

    def initGui(self):
        """Create plugin menu and toolbar entry."""
        self.action = QAction(QIcon(), "Piano Particellare", self.iface.mainWindow())
        self.action.setToolTip("Genera un piano particellare da opere e particelle catastali")
        self.action.triggered.connect(self.run)

        self.iface.addToolBarIcon(self.action)
        self.iface.addPluginToMenu("&Piano Particellare", self.action)

    def unload(self):
        """Remove plugin UI elements from QGIS."""
        if self.action:
            self.iface.removePluginMenu("&Piano Particellare", self.action)
            self.iface.removeToolBarIcon(self.action)
            self.action = None

    def run(self):
        """Open the main dialog."""
        if self.dialog is None:
            self.dialog = PianoParticellareDialog(self.iface)
        else:
            self.dialog.refresh_layers()
            self.dialog.clear_messages()

        self.dialog.setWindowModality(Qt.NonModal)
        self.dialog.show()
        self.dialog.raise_()
        self.dialog.activateWindow()

        self.iface.messageBar().pushMessage(
            "Piano Particellare",
            "Plugin pronto: configurare i layer e avviare l'elaborazione.",
            level=Qgis.Info,
            duration=3,
        )
