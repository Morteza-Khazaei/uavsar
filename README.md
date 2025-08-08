# UAVSAR Toolkit

A user-friendly tool to find, download, and process UAVSAR data.

This toolkit provides a simple command-line interface to work with UAVSAR (Uninhabited Aerial Vehicle Synthetic Aperture Radar) data from NASA's Alaska Satellite Facility (ASF). It handles the entire workflow from finding data to converting it into standard image formats that you can use in GIS software like QGIS or ArcGIS.

## Features

*   Interactively search for data by campaign, date, and processing level.
*   Download multiple products concurrently with progress bars.
*   Convert raw binary files into standard single-band GeoTIFF images.
*   Stack multiple GeoTIFFs into a single, multi-band image, perfect for creating RGB composites.

## Installation

To use this tool, you'll need to open a command-line interface.
*   On **macOS**, this is called **Terminal**.
*   On **Windows**, this is called **Command Prompt** or **PowerShell**.
*   On **Linux**, it's usually called **Terminal**.

Once you have a terminal open, follow these steps:

1.  **Get the Code**

    First, you need to download the toolkit's code using Git.
    ```bash
    git clone https://github.com/Morteza-Khazaei/uavsar.git
    cd uavsar
    ```

2.  **Create a Safe Space (Virtual Environment)**

    It's best practice to create an isolated "virtual environment" for the toolkit. This prevents it from interfering with other Python software on your computer.
    ```bash
    python -m venv .venv
    ```
    Now, activate the environment:
    *   On **macOS/Linux**: `source .venv/bin/activate`
    *   On **Windows**: `.\.venv\Scripts\activate`

3.  **Install the Toolkit**

    This final step installs the toolkit and all its dependencies, and creates the `uavsar` command.
    ```bash
    pip install -e .
    ```

## First-Time Setup: Authentication

The first time you use the `uavsar search` command, it needs to connect to NASA's Earthdata service to access the data. It will ask for your Earthdata username and password.

After you enter them, the tool will create a special file called `.netrc` in your home directory. This file securely stores your credentials so you don't have to enter them every time. The tool will not ask for your password again.

## Usage: A Step-by-Step Workflow

The toolkit provides four main commands that guide you through the data processing workflow: `search`, `unzip`, `convert`, and `stack`.

### Step 1: Find and Download Data with `search`

Use this command to find and download data from the official ASF archive.

```bash
uavsar search
```

This will start an interactive session where you will be asked to:
1.  Choose a scientific campaign (e.g., "Winnipeg, Canada").
2.  Select the type of data products you're interested in.
3.  Enter a start and end date for your search.
4.  Select the specific data files you want to download from a list of results.

The tool will then download the selected files (including `.zip` archives) into campaign- and product-specific folders.

### Step 2: Unzip Downloaded Archives with `unzip`

After downloading, use this command to extract any `.zip` files.

```bash
uavsar unzip
```

The tool will guide you in selecting which downloaded products to unzip.

### Step 3: Convert Raw Files to GeoTIFFs with `convert`

The downloaded files are in a raw binary format. To view them as images, you need to convert them to GeoTIFFs. This command does that for you.

```bash
uavsar convert
```

After it's finished, you will find new `.tiff` files inside your product folder.

### Step 4: Stack Bands into a Multi-Band Image with `stack`

This is the most creative step. You can combine several of the single-band GeoTIFFs you just created into one multi-band image. This is great for creating false-color RGB images.

Run the command and select a product folder.

```bash
uavsar stack
```

It will then show you a list of all the available `.tiff` files in that folder. You can choose which ones you want to include in your stack. The tool will then create a new file named `..._stack.tif`.