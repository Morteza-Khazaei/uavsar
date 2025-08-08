import typer
import questionary
from questionary import Separator
import logging
from datetime import datetime
from pathlib import Path

from .core import UavsarDownloader

app = typer.Typer()

def is_valid_date(date_string):
    """Validator for questionary to ensure date is in YYYY-MM-DD format."""
    try:
        datetime.fromisoformat(date_string)
        return True
    except ValueError:
        return "Please enter a date in YYYY-MM-DD format."

@app.command()
def search(
    work_dir: Path = typer.Option(
        None,
        "--work-dir",
        "-d",
        help="The directory to download and store processed files. Defaults to '~/uavsar_data'.",
        resolve_path=True,
    )
):
    """Search for and download UAVSAR data from ASF."""
    print("--- Welcome to the UAVSAR Toolkit ---")

    try:
        processor = UavsarDownloader(work_dir=work_dir)

        # 1. Select Campaign
        campaigns = processor.get_available_campaigns()
        if not campaigns:
            logging.error("Could not retrieve campaign list. Please check your connection. Exiting.")
            raise typer.Exit()
        
        selected_campaign = questionary.select("Please select a campaign:", choices=campaigns).ask()
        if not selected_campaign: raise typer.Exit()
        processor.set_campaign(selected_campaign)

        # Get the date range for the selected campaign to use as defaults
        start_date_default, end_date_default = processor.get_campaign_date_range()

        # 2. Select Processing Level
        processing_level_choices = {
            # Geocoded Data (best for current GeoTIFF conversion)
            "GRD_HD (High-Resolution Geocoded)": "GRD_HD",
            "GRD_MD (Medium-Resolution Geocoded)": "GRD_MD",
            "AMPLITUDE_GRD (Amplitude Geocoded)": "AMPLITUDE_GRD",
            "INTERFEROMETRY_GRD (Interferometry Geocoded)": "INTERFEROMETRY_GRD",

            # Slant Range Data
            "COMPLEX (Complex Slant Range)": "COMPLEX",
            "AMPLITUDE (Amplitude Slant Range)": "AMPLITUDE",
            "INTERFEROMETRY (Interferometry Slant Range)": "INTERFEROMETRY",

            # Projected Data
            "PROJECTED (Projected)": "PROJECTED",
            "PROJECTED_ML3X3 (Projected, 3x3 Multi-looked)": "PROJECTED_ML3X3",
            "PROJECTED_ML5X5 (Projected, 5x5 Multi-looked)": "PROJECTED_ML5X5",

            # Auxiliary Products
            "INC (Incidence Angle Map)": "INC",
            "SLOPE (Slope Map)": "SLOPE",
            "DEM_TIFF (Digital Elevation Model GeoTIFF)": "DEM_TIFF",
            "PAULI (Pauli Decomposition)": "PAULI",
        }
        selected_level_descriptions = questionary.checkbox(
            "Select processing levels to search for (space to select, enter to confirm):",
            choices=list(processing_level_choices.keys()),
            validate=lambda result: True if len(result) > 0 else "Please select at least one level."
        ).ask()

        if not selected_level_descriptions:
            raise typer.Exit()

        # Get the short names (e.g., 'GRD_HD') for the API call
        selected_levels = [processing_level_choices[desc] for desc in selected_level_descriptions]

        # 3. Get Date Range
        start_date = questionary.text(
            "Enter start date (YYYY-MM-DD):",
            default=start_date_default or "",
            validate=is_valid_date
        ).ask()
        if not start_date: raise typer.Exit()
        end_date = questionary.text(
            "Enter end date (YYYY-MM-DD):",
            default=end_date_default or "",
            validate=is_valid_date
        ).ask()
        if not end_date: raise typer.Exit()

        # 4. Search for data
        results = processor.search_data(start_date, end_date, selected_levels)
        if not results:
            logging.warning("No products found for the given criteria.")
            raise typer.Exit()

        # 5. Select Products to Download
        from collections import defaultdict
        product_groups = defaultdict(list)
        for i, p in enumerate(results):
            product_groups[p.properties['sceneName']].append((i, p))

        product_choices = []
        for scene_name, products_in_group in sorted(product_groups.items()):
            product_choices.append(Separator(f"--- {scene_name} ---"))
            for original_index, product in products_in_group:
                display_text = f"  [{original_index + 1}] {product.properties['processingLevel']}"
                product_choices.append({
                    'name': display_text,
                    'value': original_index  # Return the original index
                })

        selected_indices = questionary.checkbox(
            "Select products to download (space to select, enter to confirm):",
            choices=product_choices,
            validate=lambda result: True if len(result) > 0 else "Please select at least one product."
        ).ask()

        if not selected_indices:
            logging.info("No products selected. Exiting.")
            raise typer.Exit()

        # 6. Group selections by scene and download efficiently
        selected_products_by_scene = defaultdict(list)
        for idx in selected_indices:
            product = results[idx]
            selected_products_by_scene[product.properties['sceneName']].append(product)

        for scene_name, products_to_download in selected_products_by_scene.items():
            # Pass the whole list of products for the scene to the download function
            product_dir, base_name = processor.download_and_unzip_product(products_to_download)

            if not (product_dir and base_name):
                logging.warning(f"Failed to download all files for product scene {scene_name}. Skipping.")
                continue
            
            logging.info(f"Download for {base_name} complete.")
        
        print("\n--- All downloads complete. ---")
        print("Next steps: Run 'uavsar convert' to process files, then 'uavsar stack' to create multi-band images.")

    except (KeyboardInterrupt, typer.Exit):
        logging.warning("\nOperation cancelled by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

@app.command()
def convert(
    product_dir: Path = typer.Option(
        None,
        "--dir",
        "-d",
        help="Path to a specific UAVSAR product directory to convert. If not provided, will scan the default work directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    )
):
    """Converts existing, downloaded UAVSAR products to GeoTIFFs."""
    try:
        # Authentication is not needed for local conversion.
        processor = UavsarDownloader(skip_auth=True)

        dirs_to_process = []
        if product_dir:
            dirs_to_process.append(product_dir)
            print(f"--- Converting specified directory: {product_dir} ---")
        else:
            # When no specific directory is given, guide the user through the campaign structure.
            print(f"--- Scanning for campaigns in base directory: {processor.work_dir} ---")
            campaign_dirs = sorted([d for d in processor.work_dir.iterdir() if d.is_dir()])

            if not campaign_dirs:
                logging.warning(f"No campaign directories found in {processor.work_dir}. Exiting.")
                raise typer.Exit()

            selected_campaign_name = questionary.select(
                "Select a campaign to process:",
                choices=[d.name for d in campaign_dirs]
            ).ask()
            if not selected_campaign_name: raise typer.Exit()

            campaign_path = processor.work_dir / selected_campaign_name
            print(f"--- Scanning for products in: {campaign_path.name} ---")
            potential_dirs = sorted([d for d in campaign_path.iterdir() if d.is_dir() and any(d.glob('*.ann'))])

            if not potential_dirs:
                logging.warning(f"No convertible product directories found in {campaign_path.name}. Exiting.")
                raise typer.Exit()

            selected_dirs_str = questionary.checkbox(
                "Select product directories to convert:",
                choices=[d.name for d in potential_dirs],
                validate=lambda result: True if len(result) > 0 else "Please select at least one directory."
            ).ask()

            if not selected_dirs_str:
                logging.info("No directories selected. Exiting.")
                raise typer.Exit()

            dirs_to_process = [campaign_path / name for name in selected_dirs_str]

        for p_dir in dirs_to_process:
            print(f"\n--- Processing: {p_dir.name} ---")
            processor.process_product_directory(p_dir)

        print("\n--- Conversion complete. ---")
    except (KeyboardInterrupt, typer.Exit):
        logging.warning("\nOperation cancelled by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during conversion: {e}", exc_info=True)

@app.command()
def stack(
    product_dir: Path = typer.Option(
        None,
        "--dir",
        "-d",
        help="Path to a specific UAVSAR product directory to stack. If not provided, will scan the default work directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    )
):
    """Stacks selected single-band GeoTIFFs into a multi-band GeoTIFF."""
    try:
        processor = UavsarDownloader(skip_auth=True)

        dirs_to_process = []
        if product_dir:
            dirs_to_process.append(product_dir)
            print(f"--- Stacking bands for specified directory: {product_dir} ---")
        else:
            print(f"--- Scanning for campaigns in base directory: {processor.work_dir} ---")
            campaign_dirs = sorted([d for d in processor.work_dir.iterdir() if d.is_dir()])
            if not campaign_dirs:
                logging.warning(f"No campaign directories found in {processor.work_dir}. Exiting.")
                raise typer.Exit()
            selected_campaign_name = questionary.select(
                "Select a campaign to stack products from:",
                choices=[d.name for d in campaign_dirs]
            ).ask()
            if not selected_campaign_name: raise typer.Exit()

            campaign_path = processor.work_dir / selected_campaign_name
            print(f"--- Scanning for products in: {campaign_path.name} ---")
            potential_dirs = sorted([d for d in campaign_path.iterdir() if d.is_dir() and any(d.glob('*.ann'))])

            if not potential_dirs:
                logging.warning(f"No product directories with convertible files found in {campaign_path.name}. Exiting.")
                raise typer.Exit()

            selected_dirs_str = questionary.checkbox(
                "Select product directories to stack:",
                choices=[d.name for d in potential_dirs],
                validate=lambda result: True if len(result) > 0 else "Please select at least one directory."
            ).ask()
            if not selected_dirs_str: raise typer.Exit()
            dirs_to_process = [campaign_path / name for name in selected_dirs_str]

        for p_dir in dirs_to_process:
            print(f"\n--- Stacking: {p_dir.name} ---")
            
            available_tiffs = sorted(list(p_dir.glob('*.tiff')))
            if not available_tiffs:
                logging.warning(f"No .tiff files found in {p_dir}. Run 'uavsar-dl convert' on this directory first.")
                continue

            tiff_choices = [p.name for p in available_tiffs]
            selected_tiffs_str = questionary.checkbox("Select bands (GeoTIFFs) to stack:", choices=tiff_choices).ask()
            if not selected_tiffs_str: continue

            selected_tiff_paths = [p_dir / name for name in selected_tiffs_str]
            processor.stack_bands(p_dir, selected_tiff_paths)

        print("\n--- Stacking complete. ---")
    except (KeyboardInterrupt, typer.Exit):
        logging.warning("\nOperation cancelled by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during stacking: {e}", exc_info=True)