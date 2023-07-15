# Interative Report - Streamlit App

The app shows the basic aggregated metrics on a map and also offers an intuitive way to review the raw report records. The app also dynamically detects any new reports coming in and can switch between different periods. 

![](https://github.com/HiIamJeff/project-bank-credit/blob/main/assets/app_part1.gif)
![](https://github.com/HiIamJeff/project-bank-credit/blob/main/assets/app_part2.gif)

### Supplement files
- zipcode_2020_shape_simplified.geojson
  - The geojson file is built from the shape file (version 2020) from [US Census Bureau](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.2020.html#list-tab-790442341). 
  Since a 500MB file could cause longer runtime, the shape file was processed to only have data for zip codes within the NYC area and simplified geometry. The transformation was not straightforward and it is in `streamlit_script.py` for your reference.
- list_zipcode_nyc.pkl
  - The pickle file is a list built from a small but convenient package called [uszipcode](https://github.com/MacHu-GWU/uszipcode-project). It is to filter in the zip codes only within the NYC area.
  With the sample data (already limited to this area), the code here doesn't need the file. But it would be useful once you run it with the full dataset.


### Instruction
To run this, please refer to the `cli_examples.sh`
