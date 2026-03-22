# -*- coding: utf-8 -*-
"""QGIS plugin entry point for Piano Particellare."""


def classFactory(iface):
    """Load PianoParticellarePlugin class from file."""
    from .plugin import PianoParticellarePlugin

    return PianoParticellarePlugin(iface)
