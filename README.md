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
└── requirements.txt           # Dependencies

