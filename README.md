# Water Quality Prediction using Feed Forward Neural Network with Historical Flood and climate data
### MSc Data Science
**Author:** Daswadayalan M. Deenadayalan
![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![TensorFlow](https://img.shields.io/badge/TensorFlow-Keras-orange)
![Scikit-Learn](https://img.shields.io/badge/ML-Scikit--Learn-yellow)
![Status](https://img.shields.io/badge/Status-Completed-success)

## Project Overview
This project presents a robust data science framework for analyzing the dependencies between hydro-meteorological parameters (Rainfall, Temperature) and river health indices (Dissolved Oxygen, BOD) across the United Kingdom.

Using a multi-source dataset comprising decadal climate records and high-frequency water quality sampling, the project employs **Random Forest**, **XGBoost**, and **Deep Neural Networks (ANN)** to predict environmental hazards.

## Repository Structure
### UK Environment Agency (Water Quality Archive) & Met Office.

## Data Processing:
The cleaning logic is preserved in finalMain.ipynb (specifically the cleanData class).

## Training Data: 
### The data/ folder contains the pre-processed, cleaned, and engineered datasets required to run the machine learning models immediately. You do not need the raw files to run the modeling sections.
## Methodology Highlights

    EDA: Confirmed the "Dilution Effect" (Pollution concentration decreases during extreme storms) and validated Henry's Law (Temperature vs. Oxygen).

    Normalization: Applied Log-Transformation (np.log1p) to BOD data to fix severe right-skewness before Neural Network training.

    Error Testing: Implemented strict sanity filters to ensure no physical impossibilities (e.g., pH > 14 or DO < 0) entered the model.

```text
├── data/                      # PROCESSED Data (Training Ready) from uk gov
│   ├── flood_datalogs.csv     # Historical Flood Alerts
│   ├── uk_climate.csv         # Met Office Climate Variables
│   └── water_quality0.csv
    ── water_quality1.csv
    ── water_quality2.csv
    ── water_quality3.csv
    ── water_quality4.csv
     # Cleaned/Merged Water Metrics
├── finalMain.ipynb            # MAIN NOTEBOOK: ETL, EDA, and Modeling
├── README.md                  # Project Documentation
└── requirements.txt           # Dependencies '''

