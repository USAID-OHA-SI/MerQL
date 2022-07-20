
# MerQL

<!-- badges: start -->
<!-- badges: end -->

The goal of MerQL is to help OHA SI Analysts setup a local Postgres Database for PEPFAR/Datim + PANORAMA Datasets 

![sitexim-schema](https://user-images.githubusercontent.com/3952707/178600272-24b14d09-67a7-4f6b-814f-7398ed4d33f1.PNG)

# Packages

- DBI
- ODBC
- RPostgres
- RSQLite

# Configuration

Run `./Scripts/01_Setup.R` script

This file has some initial access test cases

# Load MSD Site x IM Datasets into a Star Schema like tables

Edit `./Scripts/02_MSD_Utilities.R` script to set your or preferred country name and run the rest of the script. 

**Note**: Make sure to run the script one block at the time (for the first time)

# DATIM Reference Tables

Guidance forth coming ...

---

*Disclaimer: The findings, interpretation, and conclusions expressed herein are those of the authors and do not necessarily reflect the views of United States Agency for International Development. All errors remain our own.*

