# Piano Particellare - QGIS Plugin

## Folder structure

```text
piano_particellare/
├── __init__.py
├── dialog.py
├── metadata.txt
├── plugin.py
└── processor.py
```

## Installation in QGIS 3.40

1. Copy the `piano_particellare` folder into your QGIS profile plugin directory.
   - Linux: `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins/`
   - Windows: `%APPDATA%\QGIS\QGIS3\profiles\default\python\plugins\`
   - macOS: `~/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins/`
2. Start QGIS 3.40.
3. Open **Plugins > Manage and Install Plugins... > Installed**.
4. Enable **Piano Particellare**.
5. Run the plugin from the **Plugins** menu or the toolbar.

## Functional overview

The plugin:

- requires one polygon cadastral layer and one or more polygon opere layers;
- validates CRS equality across every input layer;
- optionally fixes invalid geometries with `makeValid()`;
- intersects each opere feature against cadastral parcels;
- explodes every result to singlepart polygon features;
- writes SHP or GPKG output with the required attributes;
- optionally adds the output layer to the project;
- writes a detailed text log next to the output dataset.

## Notes

- Input layers must all share the same CRS.
- Only polygon layers are accepted.
- Area is computed using the CRS units of the input/output layer.
- If `id_opera` is not mapped, the plugin generates values like `layername_featureid`.
