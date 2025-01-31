# Study Data
## Files

| File/Folder | Description | Data source |
| ----------- | ----------- | ----------- |
| ./swissboundaries/swissBOUNDARIES3D_1_1_TLM_HOHEITSGEBIET.shp | Folder containing a ESRI Shapefile with the Swiss cantonal boundaries. | swissBOUNDARIES3D |
| hangmuren_export_2023-04-10-10-06_cleaned.csv | Inventory data from the WSL Hangmurendatenbank with added covariate values. | Hangmuren |
| SpontRutschBE_cleaned.csv | Inventory data from the orthophoto-based inventory of the canton Bern with added covariate values. | SpontRutschBE |
| storme_interlis_cleaned.csv | Inventory data from StorMe database with added covariate values. | StorMe |
| be_sampled_rock_20240719.csv, hmdb_sampled_rock_20240719.csv, storme_sampled_rock_20240719.csv | CSV files with points of 0 m failure depth sampled within rock signature. | - |
| gc_unconsolidated_deposits_LITHO_mappings.csv | Mapping table for assigning labels, group labels, and expert derived susceptibility mappings to unconsolidated deposits from Swisstopo Geocover. Only used in tests but not in the final model. | - |
| gk500_LITHO_mappings.csv | Mapping table for assigning labels and expert derived susceptibility mappings to lithologies  from the Swisstopo GK500 map. Only used in tests but not in the final model. | -  |


## Data sources
- **swissBOUNDARIES3D**: Federal Office of Topography swisstopo, swissBOUNDARIES3D, https://www.swisstopo.admin.ch/en/landscape-model-swissboundaries3d, 2024.
- **Hangmuren**: Eidgenössische Forschungsanstalt für Wald, Schnee und Landschaft WSL, Datenbank flachgründige Rutschungen und Hangmuren,  2024, https://www.wsl.ch/de/services-produkte/datenbank-flachgruendige-rutschungen-und-hangmuren/, 2024.
- **SpontRutschBE**: Hählen, Nils, Kennzahlen zu spontanen Rutschungen im Kanton Bern mit Schwerpunkt auf Alpen und Voralpen, https://www.researchgate.net/publication/368510037, 2023.
- **StorMe**: Bundesamt für Umwelt BAFU, Naturereigniskataster StorMe,  https://www.bafu.admin.ch/bafu/de/home/themen/naturgefahren/fachinformationen/naturgefahrensituation-und-raumnutzung/gefahrengrundlagen/naturereigniskataster-storme.html
