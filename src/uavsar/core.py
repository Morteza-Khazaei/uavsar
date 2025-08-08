import asf_search
import getpass
import logging
import numpy as np
import os
import pandas as pd
import pytz
import rasterio
import re
from rich.progress import Progress, BarColumn, TextColumn, TransferSpeedColumn, TimeRemainingColumn
import zipfile
from pathlib import Path
from rasterio.transform import Affine
from rasterio.crs import CRS

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

class UavsarDownloader:
    """
    A class to search, download, and process UAVSAR GRD data from the ASF
    data center for any specified campaign.
    """

    def __init__(self, work_dir=None, skip_auth=False):
        """
        Initializes the UavsarDownloader.

        Args:
            work_dir (str, optional): The directory to download and store
                                      processed files. Defaults to '~/uavsar_data'.
            skip_auth (bool, optional): If True, skips authentication. Useful for
                                        local processing only. Defaults to False.
        """
        self.session = None
        if not skip_auth:
            self._setup_auth()
            self.session = asf_search.ASFSession()

        if work_dir:
            self.base_work_dir = Path(work_dir).resolve()
        else:
            self.base_work_dir = Path.home() / 'uavsar_data'

        self.base_work_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir = self.base_work_dir # Default work_dir is the base
        logging.info(f"Base working directory set to: {self.base_work_dir}")

        self.campaign = None
        self.search_results = None

    def _setup_auth(self):
        """
        Sets up authentication by checking for env vars, a .netrc file, or prompting the user.
        """
        netrc_path = Path.home() / '.netrc'

        # Priority 1: .netrc file
        if netrc_path.exists():
            logging.info("Authentication file (.netrc) found.")
            return

        # Priority 2: Environment variables
        if 'EARTHDATA_USERNAME' in os.environ and 'EARTHDATA_PASSWORD' in os.environ:
            logging.info("Using Earthdata credentials from environment variables.")
            return

        # Priority 3: Prompt user to create .netrc
        logging.info("ASF credentials not found. Please provide them to create a .netrc file.")
        username = input("Enter your Earthdata Login Username: ")
        password = getpass.getpass("Enter your Earthdata Login Password: ")

        with open(netrc_path, 'w') as f:
            f.write(f"machine urs.earthdata.nasa.gov login {username} password {password}\n")
        os.chmod(netrc_path, 0o600)
        logging.info(f"Successfully created .netrc file at {netrc_path}")

    def get_available_campaigns(self):
        """Fetches a list of available UAVSAR campaigns from ASF."""
        logging.info("Fetching available UAVSAR campaigns...")
        try:
            # asf_search.campaigns with a platform specified returns a list of campaigns.
            campaigns = asf_search.campaigns(platform=asf_search.PLATFORM.UAVSAR)
            # Sort campaigns for user-friendly display
            return sorted(list(set(campaigns)))
        except Exception as e:
            logging.error(f"Could not fetch campaign list: {e}")
            return []

    def set_campaign(self, campaign: str):
        """Sets the campaign for the downloader and updates the working directory."""
        self.campaign = campaign
        # Sanitize the campaign name to make it a valid directory name
        sanitized_campaign_name = re.sub(r'[^\w\s-]', '', campaign).strip().replace(' ', '_')
        self.work_dir = self.base_work_dir / sanitized_campaign_name
        self.work_dir.mkdir(exist_ok=True)
        logging.info(f"Campaign set. Files will be stored in: {self.work_dir}")

    def get_campaign_date_range(self):
        """
        Fetches the start and end dates for the currently set campaign by searching
        for all products within it.
        """
        if not self.campaign:
            return None, None
            
        logging.info(f"Fetching date range for campaign: {self.campaign}...")
        try:
            # To find the full date range, we must search the entire campaign.
            # This might take a moment.
            results = asf_search.search(
                platform=asf_search.PLATFORM.UAVSAR,
                campaign=self.campaign,
            )
            
            if not results:
                logging.warning(f"No products found for campaign '{self.campaign}' to determine date range.")
                return None, None

            all_start_times = [pd.to_datetime(p.properties['startTime']) for p in results]
            all_stop_times = [pd.to_datetime(p.properties['stopTime']) for p in results]

            start_date = min(all_start_times).strftime('%Y-%m-%d')
            end_date = max(all_stop_times).strftime('%Y-%m-%d')
            
            logging.info(f"Date range for {self.campaign}: {start_date} to {end_date}")
            return start_date, end_date

        except Exception as e:
            logging.warning(f"Could not determine date range for {self.campaign}: {e}")
            return None, None

    def search_data(self, start_date: str, end_date: str, processing_levels: list):
        """
        Searches for UAVSAR data for the specified campaign within a date range.

        Args:
            start_date: The start date in 'YYYY-MM-DD' format.
            end_date: The end date in 'YYYY-MM-DD' format.
            processing_levels: A list of processing levels to search for (e.g., ['GRD_HD']).
        """
        logging.info(f"Searching for {self.campaign} data from {start_date} to {end_date}...")
        try:
            # Pass the authenticated session to the search function
            self.search_results = asf_search.search(
                platform=asf_search.PLATFORM.UAVSAR,
                campaign=self.campaign,
                processingLevel=processing_levels,
                start=start_date,
                end=end_date,
            )
            logging.info(f"Found {len(self.search_results)} products.")
            return self.search_results

        except Exception as e:
            logging.error(f"An error occurred during search: {e}", exc_info=True)
            self.search_results = asf_search.ASFSearchResults([])
            return self.search_results

    def download_and_unzip_product(self, products_for_scene: list):
        """
        Downloads and unzips all selected files for a given scene.

        Returns:
            A tuple of (Path, str) for the product directory and base name, or (None, None) on failure.
        """
        if not products_for_scene:
            return None, None

        product_name = products_for_scene[0].properties['sceneName']
        logging.info(f"Processing product scene: {product_name}")
        base_name = product_name.replace('_grd', '')

        # 1. Collect all URLs from the products selected by the user for this scene
        urls_to_download = {p.properties['url'] for p in products_for_scene}
        
        # 2. Check which files already exist and don't need to be downloaded
        unzipped_files_dir = self.work_dir / base_name
        unzipped_files_dir.mkdir(exist_ok=True)
        
        final_urls_to_download = []
        files_for_unzipping = []
        
        for url in urls_to_download:
            filename = url.split('/')[-1]
            staging_path = self.work_dir / filename
            
            is_zip = filename.lower().endswith('.zip')
            if not is_zip and (unzipped_files_dir / filename).exists():
                logging.info(f"File {filename} already exists in product directory. Skipping.")
                continue
            
            if staging_path.exists():
                logging.info(f"File {filename} already exists in working directory. Skipping download.")
                files_for_unzipping.append(staging_path)
                continue
                
            final_urls_to_download.append(url)

        # 3. Download and Unzip with Progress Bars
        with Progress(
            TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
        ) as progress:
            if final_urls_to_download:
                logging.info(f"Downloading {len(final_urls_to_download)} file(s) for {base_name}...")
                for url in final_urls_to_download:
                    filename = url.split('/')[-1]
                    staging_path = self.work_dir / filename
                    task_id = progress.add_task("download", filename=filename, start=False)
                    try:
                        # Use the authenticated session to stream the download
                        response = self.session.get(url, stream=True, timeout=120)
                        response.raise_for_status()
                        
                        total_size = int(response.headers.get('content-length', 0))
                        progress.update(task_id, total=total_size)
                        progress.start_task(task_id)
                        
                        with open(staging_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                                progress.update(task_id, advance=len(chunk))
                        
                        files_for_unzipping.append(staging_path)
                    except Exception as e:
                        logging.error(f"Failed to download {filename}: {e}")
                        progress.update(task_id, description=f"[bold red]Failed: {filename}[/bold red]")
                        if staging_path.exists():
                            staging_path.unlink(missing_ok=True)
            
            # Unzip downloaded files
            for file_path in filter(lambda p: p.exists() and zipfile.is_zipfile(p), files_for_unzipping):
                self._unzip_with_progress(file_path, unzipped_files_dir, progress)

        if not files_for_unzipping:
            logging.warning(f"No files were successfully downloaded or found for product {product_name}.")
            return None, None

        # 4. Move any raw (non-zip) files
        for file_path in files_for_unzipping:
            if not file_path.exists():
                logging.error(f"Downloaded file {file_path.name} not found. Download may have failed silently.")
                continue
                
            if zipfile.is_zipfile(file_path):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    # Unzipping is now handled in the progress block
                    pass
            else: # Handle raw files that weren't zipped
                destination_path = unzipped_files_dir / file_path.name
                if not destination_path.exists():
                    file_path.rename(destination_path)

        return unzipped_files_dir, base_name
    
    def _unzip_with_progress(self, zip_path: Path, extract_dir: Path, progress: Progress):
        """Unzips a file while updating a rich progress bar."""
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            infolist = zip_ref.infolist()
            if not infolist: return

            # Check if all files from the zip exist in the target directory
            if all((extract_dir / f.filename).exists() for f in infolist):
                logging.info(f"Contents of {zip_path.name} already exist. Skipping unzip.")
                return

            task_id = progress.add_task("unzip", filename=f"Unzipping {zip_path.name}", total=len(infolist))
            for member in infolist:
                zip_ref.extract(member, extract_dir)
                progress.update(task_id, advance=1)
    @staticmethod
    def _get_encapsulated(str_line, encapsulator):
        """Helper to find text within encapsulators (e.g., parentheses)."""
        result = []
        if len(encapsulator) > 2:
            raise ValueError('encapsulator can only be 1 or 2 chars long!')
        lcap, rcap = (encapsulator[0], encapsulator[1]) if len(encapsulator) == 2 else (encapsulator, encapsulator)
        
        if lcap in str_line:
            for i, val in enumerate(str_line.split(lcap)):
                if i != 0:
                    result.append(val[0:val.index(rcap)])
        return result

    @staticmethod
    def _read_annotation(ann_file: Path) -> dict:
        """
        Parses a UAVSAR annotation file into a structured dictionary.
        Logic adapted from the uavsar_pytools library.
        """
        with open(ann_file) as fp:
            lines = fp.readlines()
        data = {}

        for line in lines:
            info = line.strip().split(';')
            comment = info[-1].strip().lower()
            info = info[0]
            if info and "=" in info:
                d = info.split('=')
                name, value = d[0], d[1]
                key = name.split('(')[0].strip().lower()
                units = UavsarDownloader._get_encapsulated(name, '()')
                units = units[0] if units else None
                value = value.strip()

                # Attempt to convert value to a numeric type (int or float)
                try:
                    numeric_value = float(value)
                    # Convert to int if it's a whole number to avoid '.0'
                    if numeric_value.is_integer():
                        value = int(numeric_value)
                    else:
                        value = numeric_value
                except (ValueError, TypeError):
                    pass  # Value is a string, leave it as is

                data[key] = {'value': value, 'units': units, 'comment': comment}

        # Convert times to datetimes
        if 'start time of acquisition' in data:
            for timing in ['start', 'stop']:
                key = f'{timing} time of acquisition'
                if key in data:
                    dt = pd.to_datetime(data[key]['value'])
                    dt = dt.astimezone(pytz.timezone('US/Mountain'))
                    data[key]['value'] = dt
        return data
    
    def process_product_directory(self, product_dir: Path):
        """
        Finds all convertible data files in a directory and processes them into GeoTIFFs.

        Args:
            product_dir (Path): The path to the directory containing the unzipped product files.
        """
        logging.info(f"Processing all convertible files in: {product_dir}")

        # 1. Find and parse the annotation file, which contains metadata for all other files.
        try:
            ann_file = next(product_dir.glob('*.ann'))
            logging.info(f"Found annotation file: {ann_file.name}")
        except StopIteration:
            logging.error(f"No .ann file found in {product_dir}. Cannot process files.")
            return

        ann_data = self._read_annotation(ann_file)
        if not ann_data:
            logging.error(f"Failed to parse annotation file: {ann_file.name}")
            return

        # 2. Find all relevant data files to convert
        files_to_convert = [
            f for f in product_dir.iterdir() 
            if f.suffix.lower() in ['.grd', '.slc', '.mlc', '.inc', '.hgt', '.slope', '.dem', '.amp', '.cor', '.unw']
        ]

        if not files_to_convert:
            logging.info("No convertible files found in product directory.")
            return

        # 3. Convert files with a progress bar
        with Progress(
            TextColumn("[bold green]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "({task.completed} of {task.total})"
        ) as progress:
            task = progress.add_task("Converting to GeoTIFF...", total=len(files_to_convert))
            for data_file in files_to_convert:
                self._convert_file_to_geotiff(data_file, ann_data)
                progress.advance(task)
    
    def _get_band_metadata(self, in_fp: Path, ann_data: dict) -> dict | None:
        """
        Parses annotation data to get all required metadata for a given band file.
        Logic adapted from the uavsar_pytools library.

        Args:
            in_fp (Path): Path to the input binary data file (e.g., .grd, .inc).
            ann_data (dict): Parsed annotation data from _read_annotation.
        """
        file_type = in_fp.suffix.lstrip('.')
        fname = in_fp.stem
        
        # Find the correct metadata prefix in the annotation dictionary. This can be tricky.
        search_key_prefix = None
        if file_type in ['slope', 'inc', 'hgt']:
            # Ancillary files often share dimensions with other layers but lack their own metadata block.
            # We create a fallback chain to find dimensions from a related layer.
            fallback_keys = [file_type, 'hgt', 'grd_pwr']
            for key in fallback_keys:
                if f'{key}.set_rows' in ann_data:
                    search_key_prefix = key
                    logging.debug(f"For file type '{file_type}', using metadata from '{key}' block.")
                    break
        else:
            # For other types (slc, mlc, grd), find the first available metadata block.
            # The dimensions are usually consistent across _pwr, _mag, _phase, etc.
            for suffix in ['_pwr', '_mag', '_phase', '']:
                # For grd, the key might be grd_pwr. For slc, it might be slc_mag.
                test_key = f"{file_type}{suffix}".strip('_') if suffix else file_type
                if f"{test_key}.set_rows" in ann_data:
                    search_key_prefix = test_key
                    break

        if not search_key_prefix:
            logging.debug(f"Could not find metadata for file '{in_fp.name}' in annotation file. Skipping conversion.")
            return

        try:
            rows = ann_data[f'{search_key_prefix}.set_rows']['value']
            cols = ann_data[f'{search_key_prefix}.set_cols']['value']

            # Determine data type from annotation file, not filename heuristics
            val_frmt = ann_data.get(f'{search_key_prefix}.val_frmt', {}).get('value', '').upper()
            is_complex_from_meta = 'COMPLEX' in val_frmt
            
            if 'REAL*4' in val_frmt or 'REAL' in val_frmt:
                dtype = np.float32
            elif 'REAL*8' in val_frmt: # For robustness
                dtype = np.float64
            elif is_complex_from_meta:
                dtype = np.complex64
            else:
                # Fallback for formats like .inc, .hgt that might not have val_frmt
                dtype = np.float32

            # Override for GRD cross-products which are complex, even if the base metadata (grd_pwr) is for a real type.
            if file_type == 'grd':
                # Extract polarization from filename, e.g., HHHV, HVHV, etc.
                pol_match = re.search(r'L\d{3}([HV]{4})_CX', fname)
                if pol_match:
                    polarization = pol_match.group(1)
                    # Co-pol power products are real. Cross-pol products are complex.
                    if polarization not in ['HHHH', 'VVVV', 'HVHV', 'VHVH']:
                        dtype = np.complex64
                        is_complex = True
                    else:
                        is_complex = False # It's a real power image
                else:
                    is_complex = is_complex_from_meta
            else:
                is_complex = is_complex_from_meta
            
            # Get nodata value from annotation, with a fallback default.
            nodata_key = f'{search_key_prefix}.no_data'
            nodata_entry = ann_data.get(nodata_key)
            if nodata_entry:
                nodata_value = nodata_entry['value']
            else:
                # If no_data is not specified, use a smart default based on file type
                if file_type in ['hgt', 'dem', 'inc', 'slope']:
                    # For elevation or angle maps, 0 is a valid value. Use a large negative number.
                    nodata_value = -10000.0
                else:
                    # For power/magnitude images (grd, slc, mlc), 0.0 is a common no-data value.
                    nodata_value = 0.0 
            
            transform = None
            crs = None

            # --- GEOREFERENCING LOGIC ---
            # Attempt to find UTM projection info first
            utm_geo_prefix = None
            for p in [search_key_prefix, f'{file_type}_pwr', file_type]:
                if f'{p}.upper_left_easting' in ann_data:
                    utm_geo_prefix = p
                    break
            
            if utm_geo_prefix:
                logging.debug(f"Found UTM projection info with prefix '{utm_geo_prefix}'")
                ul_easting = ann_data[f'{utm_geo_prefix}.upper_left_easting']['value']
                ul_northing = ann_data[f'{utm_geo_prefix}.upper_left_northing']['value']
                easting_ps = ann_data[f'{utm_geo_prefix}.easting_pixel_spacing']['value']
                northing_ps = ann_data[f'{utm_geo_prefix}.northing_pixel_spacing']['value']
                utm_zone = ann_data['peg_point_utm_zone']['value']
                latitude = ann_data['peg_point_latitude']['value']
                
                epsg_code = 32600 + utm_zone if latitude >= 0 else 32700 + utm_zone
                crs = CRS.from_epsg(epsg_code)
                transform = Affine(easting_ps, 0, ul_easting, 0, -northing_ps, ul_northing)
            else:
                # Attempt to find EQA (lat/lon) projection info
                eqa_geo_prefix = None
                for p in [search_key_prefix, f'{file_type}_pwr', file_type]:
                    # EQA uses 'row_addr' with 'deg' units for lat
                    key = f'{p}.row_addr'
                    if key in ann_data and ann_data[key].get('units') == 'deg':
                        eqa_geo_prefix = p
                        break
                
                if eqa_geo_prefix:
                    logging.debug(f"Found EQA projection info with prefix '{eqa_geo_prefix}'")
                    ul_lat = ann_data[f'{eqa_geo_prefix}.row_addr']['value']
                    ul_lon = ann_data[f'{eqa_geo_prefix}.col_addr']['value']
                    lat_ps = ann_data[f'{eqa_geo_prefix}.row_mult']['value']
                    lon_ps = ann_data[f'{eqa_geo_prefix}.col_mult']['value']

                    crs = CRS.from_epsg(4326) # WGS 84
                    transform = Affine(lon_ps, 0, ul_lon, 0, lat_ps, ul_lat)

            if not transform:
                return None

            return {
                'rows': rows, 'cols': cols, 'dtype': dtype, 'is_complex': is_complex,
                'nodata_value': nodata_value, 'crs': crs, 'transform': transform,
                'search_key_prefix': search_key_prefix
            }

        except KeyError as e:
            logging.error(f"Missing required metadata key {e} for '{search_key_prefix}' in annotation file.")
            return None

    def _convert_file_to_geotiff(self, in_fp: Path, ann_data: dict):
        """ 
        Converts a single UAVSAR binary file to a GeoTIFF using its annotation metadata.

        Args:
            in_fp (Path): Path to the input binary data file (e.g., .grd, .inc).
            ann_data (dict): Parsed annotation data from _read_annotation.
        """
        out_fp = in_fp.with_suffix(in_fp.suffix + '.tiff')
        if out_fp.exists():
            logging.info(f"Output file {out_fp.name} already exists. Skipping conversion.")
            return

        metadata = self._get_band_metadata(in_fp, ann_data)
        if not metadata:
            logging.warning(f"Could not get metadata for {in_fp.name}. Skipping conversion.")
            return

        logging.info(f"Converting {in_fp.name} using metadata key prefix: '{metadata['search_key_prefix']}'")

        try: # This try block should use the dtype determined above
            data = np.fromfile(in_fp, dtype=metadata['dtype'])
            if metadata['is_complex']:
                data = np.abs(data) # Convert complex to magnitude for visualization
            data = data.reshape((metadata['rows'], metadata['cols']))
        except Exception as e:
            logging.error(f"Failed to read or process binary data from {in_fp.name}: {e}")
            return

        profile = {
            'driver': 'GTiff',
            'height': metadata['rows'],
            'width': metadata['cols'],
            'count': 1,
            'dtype': data.dtype,
            'crs': metadata['crs'],
            'transform': metadata['transform'],
            'compress': 'lzw',
            'nodata': metadata['nodata_value'],
        }

        logging.info(f"Writing GeoTIFF to {out_fp.name}")
        with rasterio.open(out_fp, 'w', **profile) as dst:
            dst.write(data, 1)
        
        logging.info(f"Successfully created GeoTIFF: {out_fp.name}")

    def stack_bands(self, product_dir: Path, tiff_paths: list[Path]):
        """
        Stacks a list of single-band GeoTIFFs into a single multi-band GeoTIFF.
        """
        if not tiff_paths:
            logging.warning("No GeoTIFF files provided for stacking.")
            return

        # 1. Read band data and get metadata from the first band
        bands_data = []
        band_names = []
        first_band_profile = None

        with Progress(
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(),
            "({task.completed} of {task.total})"
        ) as progress:
            task = progress.add_task("Reading bands...", total=len(tiff_paths))
            for tiff_path in tiff_paths:
                try:
                    with rasterio.open(tiff_path) as src:
                        if not first_band_profile:
                            first_band_profile = src.profile
                            if src.count != 1:
                                logging.error(f"Input file {tiff_path.name} is not a single-band GeoTIFF. Aborting stack.")
                                return
                        else:
                            if (src.width != first_band_profile['width'] or
                                src.height != first_band_profile['height'] or
                                src.crs != first_band_profile['crs'] or
                                src.transform != first_band_profile['transform']):
                                logging.error(f"Dimension or CRS mismatch: {tiff_path.name} does not match the first band. Aborting stack.")
                                return
                        
                        bands_data.append(src.read(1))
                        band_names.append(tiff_path.stem)
                        progress.advance(task)

                except Exception as e:
                    logging.error(f"Failed to read or process data from {tiff_path.name}: {e}")
                    return
        
        # 2. Create the output GeoTIFF
        out_fp = product_dir / f"{product_dir.name}_stack.tif"
        if out_fp.exists():
            logging.info(f"Output stack file {out_fp.name} already exists. Skipping.")
            return

        # Update the profile for the new multi-band file
        output_profile = first_band_profile.copy()
        output_profile['count'] = len(bands_data)
        output_profile['compress'] = 'deflate'
        output_profile['predictor'] = 3 # Predictor for floating point data
        
        logging.info(f"Writing {len(bands_data)}-band GeoTIFF to {out_fp.name}")
        with rasterio.open(out_fp, 'w', **output_profile) as dst:
            for i, (band_data, band_name) in enumerate(zip(bands_data, band_names), 1):
                dst.write(band_data, i)
                dst.set_band_description(i, band_name)
        
        logging.info(f"Successfully created stacked GeoTIFF: {out_fp.name}")