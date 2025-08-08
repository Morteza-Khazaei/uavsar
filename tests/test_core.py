import pytest
import numpy as np
import rasterio
from pathlib import Path
from uavsar.core import UavsarDownloader

@pytest.fixture
def processor():
    """Fixture to create a UavsarDownloader instance for testing."""
    return UavsarDownloader(skip_auth=True)

@pytest.fixture
def sample_ann_content():
    """Provides the content of a sample annotation file."""
    return """; Test annotation file
grd_pwr.set_rows (pixels) = 10
grd_pwr.set_cols (pixels) = 15
grd_pwr.row_addr (deg) = 50.0
grd_pwr.col_addr (deg) = -100.0
grd_pwr.row_mult (deg/pixel) = -5.556e-05
grd_pwr.col_mult (deg/pixel) = 5.556e-05
grd_pwr.val_frmt (&) = REAL*4
grd_pwr.no_data (dB) = -10000.0
inc.set_rows (pixels) = 10
inc.set_cols (pixels) = 15
inc.val_frmt (&) = REAL*4
"""

@pytest.fixture
def temp_product_dir(tmp_path, sample_ann_content):
    """Creates a temporary product directory with an annotation file and dummy data."""
    # This represents the main product directory, e.g., UA_winnip_...
    main_product_dir = tmp_path / "UA_test_product_scene_01"
    main_product_dir.mkdir()

    # This represents the unzipped data directory, e.g., winnip_..._grd
    unzipped_data_dir = main_product_dir / "test_product_grd"
    unzipped_data_dir.mkdir()

    # Create annotation file inside the unzipped data directory
    ann_path = unzipped_data_dir / "test.ann"
    ann_path.write_text(sample_ann_content)

    # Create dummy binary data files inside the unzipped data directory
    # (10 rows * 15 cols) * 4 bytes/pixel = 600 bytes
    (unzipped_data_dir / "test.grd").write_bytes(np.zeros(150, dtype=np.float32).tobytes())
    (unzipped_data_dir / "test.inc").write_bytes(np.zeros(150, dtype=np.float32).tobytes())

    return unzipped_data_dir # Return the path to the unzipped data

def test_read_annotation(processor, sample_ann_content, tmp_path):
    """Tests the parsing of an annotation file."""
    ann_path = tmp_path / "test.ann"
    ann_path.write_text(sample_ann_content)

    ann_data = processor._read_annotation(ann_path)

    # Test a few key values
    assert ann_data['grd_pwr.set_rows']['value'] == 10
    assert ann_data['grd_pwr.row_mult']['value'] == -0.00005556
    assert ann_data['grd_pwr.val_frmt']['value'] == 'REAL*4'
    assert ann_data['grd_pwr.no_data']['value'] == -10000.0

def test_convert_file_to_geotiff(processor, temp_product_dir):
    """Tests the conversion of a single binary file to a GeoTIFF."""
    ann_path = temp_product_dir / "test.ann"
    ann_data = processor._read_annotation(ann_path)
    grd_path = temp_product_dir / "test.grd"

    processor._convert_file_to_geotiff(grd_path, ann_data)

    # Check if the output file was created
    out_tiff_path = temp_product_dir / "test.grd.tiff"
    assert out_tiff_path.exists()

    # Check the properties of the created GeoTIFF
    with rasterio.open(out_tiff_path) as src:
        assert src.count == 1
        assert src.height == 10
        assert src.width == 15
        assert src.crs == rasterio.crs.CRS.from_epsg(4326)
        assert src.nodata == -10000.0
        assert src.transform.a == 5.556e-05  # Pixel width
        assert src.transform.e == -5.556e-05 # Pixel height

def test_convert_ancillary_file_with_fallback(processor, temp_product_dir):
    """
    Tests that an ancillary file (like .inc) can be converted by borrowing
    metadata from another layer (like grd_pwr).
    """
    ann_path = temp_product_dir / "test.ann"
    # Modify the ann_data to remove the specific 'inc' block, forcing a fallback
    ann_data = processor._read_annotation(ann_path)
    del ann_data['inc.set_rows']
    del ann_data['inc.set_cols']
    del ann_data['inc.val_frmt']

    inc_path = temp_product_dir / "test.inc"
    processor._convert_file_to_geotiff(inc_path, ann_data)

    out_tiff_path = temp_product_dir / "test.inc.tiff"
    assert out_tiff_path.exists()

    with rasterio.open(out_tiff_path) as src:
        assert src.height == 10
        assert src.width == 15

@pytest.fixture
def stacked_tiffs_dir(temp_product_dir):
    """Creates a directory with multiple single-band GeoTIFFs for stacking."""
    processor = UavsarDownloader(skip_auth=True)
    # temp_product_dir is the unzipped data directory
    ann_path = temp_product_dir / "test.ann"
    ann_data = processor._read_annotation(ann_path)

    # Create a few dummy files to convert
    grd_path = temp_product_dir / "winnip_HHHH.grd"
    grd_path.write_bytes(np.ones(150, dtype=np.float32).tobytes())
    
    inc_path = temp_product_dir / "winnip.inc"
    inc_path.write_bytes(np.full(150, 0.5, dtype=np.float32).tobytes())

    processor._convert_file_to_geotiff(grd_path, ann_data)
    processor._convert_file_to_geotiff(inc_path, ann_data)

    return temp_product_dir

def test_stack_bands(processor, stacked_tiffs_dir):
    """Tests stacking multiple GeoTIFFs into a single file."""
    # stacked_tiffs_dir is the unzipped data directory. Its parent is the main product directory.
    main_product_dir = stacked_tiffs_dir.parent 
    
    tiff_paths = [
        stacked_tiffs_dir / "winnip_HHHH.grd.tiff",
        stacked_tiffs_dir / "winnip.inc.tiff"
    ]

    # The first argument to stack_bands is the unzipped data directory
    processor.stack_bands(stacked_tiffs_dir, tiff_paths)

    # The output file should be in the parent directory
    out_stack_path = main_product_dir / f"{main_product_dir.name}_stack.tif"
    assert out_stack_path.exists()

    with rasterio.open(out_stack_path) as src:
        assert src.count == 2
        assert src.height == 10
        assert src.width == 15
        assert src.descriptions == ("winnip_HHHH.grd", "winnip.inc")

        # Check content of the bands
        band1_data = src.read(1)
        band2_data = src.read(2)
        assert np.all(band1_data == 1.0)
        assert np.all(band2_data == 0.5)