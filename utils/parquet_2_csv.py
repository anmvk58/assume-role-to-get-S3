import os
import glob
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
# Exception import
from pyarrow.lib import ArrowInvalid


# This function support convert Decimal Type Column to String with non-scientific notation
from utils.utils import replace_date

logger = logging.getLogger("Utils_Module")

def custom_cast(column):
    formatted_strings = []
    for value in column:
        if value is not None and value.is_valid:
            formatted_strings.append(f'{value.as_py():.10f}')
        else:
            formatted_strings.append(None)
    return pa.array(formatted_strings, type=pa.string())


# This function read multiple parquets file and write to one csv file
def process_parquet_2_csv(config, data_date):
    logger.info(f"Process item: {config.get('file_out_name')}")
    list_file: list = list(
        filter(lambda x: x.endswith('parquet'), glob.glob(os.path.join(config.get("path_out"), '*.parquet'))))
    logger.info("List file Parquets: {}".format(list_file))

    if len(list_file) == 0:
        raise LookupError("Not found any parquet file !")
        return

    pa_tables = []
    for file_name in list_file:
        pa_tables.append(pq.read_table(file_name))

    # Finish read *.parquet
    combined_table: pa.Table = pa.concat_tables(pa_tables)

    # Convert all column to String type to export:
    fields = [(field.name, pa.string()) for field in combined_table.schema]
    new_schema = pa.schema(fields)

    new_columns = []
    for column in combined_table.itercolumns():
        # Convert Decimal Type to String Type
        if pa.types.is_decimal(column.type):
            new_column = custom_cast(column)
            new_columns.append(new_column)
        else:
            new_columns.append(column)

    # Create new Pyarrow Table base on new columns
    table_casted = pa.table(new_columns, schema=new_schema)

    # Begin transform if had any config
    if 'transform' in config:
        # Rename column:
        if 'rename_column' in config['transform']:
            if 'org_value' not in config['transform']['rename_column'] or \
                    'new_value' not in config['transform']['rename_column']:
                logger.error("Config is not valid at keys: [rename_column]")
                raise ValueError('Config in rename_column transform is wrong !!!')
            else:
                org_val = config['transform']['rename_column']['org_value'].split(',')
                new_val = config['transform']['rename_column']['new_value'].split(',')
                rename_dict = dict(zip(org_val, new_val))

                # Convert to Original Header and Format
                table_casted = table_casted.select(org_val)
                table_casted = table_casted.rename_columns(rename_dict)

    # Write to csv file (need to config exactly in SAP Env)
    file_name = replace_date(config.get("file_out_name"), data_date)
    out_path = "{}{}".format(config["path_out"], file_name)
    delimiter = "," if 'delimiter_out' not in config else config['delimiter_out']
    try:
        # Try to export csv without quote (original as ERP require)
        write_options = csv.WriteOptions(include_header=True, delimiter=delimiter, quoting_style="none",
                                         batch_size=50000)
        csv.write_csv(data=table_casted, output_file=f'{out_path}', write_options=write_options)

    except ArrowInvalid:
        # File must be quote with all column and row
        logger.warning(f"File [{file_name}] has to quote all row and column")
        write_options = csv.WriteOptions(include_header=True, delimiter=delimiter, quoting_style="needed",
                                         batch_size=50000)
        csv.write_csv(data=table_casted, output_file=f'{out_path}', write_options=write_options)

    except Exception as e:
        logger.error("Error when writing csv file output")
        logger.error("###ERROR###: " + str(e))


# This function read multiple parquets file and write to many csv files
# This is more effective RAM, use when first method can not run
def process_parquet_2_csv_w2():
    pass
