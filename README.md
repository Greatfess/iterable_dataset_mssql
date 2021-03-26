# **IterableDataset for pytorch from MS SQL Server**

This is the implementation of torch.utils.data.IterableDataset that iterates over rows from mssql server.<br/>
Fully compatible with torch.utils.data.DataLoader<br/>
Class includes collate_fn function to convert sql types to python.

# Parameters
## Reqiured:
- server (server name or ip address)
- database (database name)
- table (table name)
- iterable_column
## Optional:
- iterate_from (value to start iteration over iterable_column)
- iterate_to (value to end iteration over iterable_column)
- username (if None, then trusted connection is used)
- password (if None, then trusted connection is used)
- columns (list of columns to include in query)
- exc_cols (list of columns to exclude from query)
- dt_to_str (convert datetime columns to str)
- charset (connection charset, utf-8 by default)

# Quick start
```python
args = {
    'server': r'<your_server_name_or_ip>',
    'database': '<your_database_name>',
    'table': '<your_table_name>',
    'iterable_column': '<column_to_iterate_over>'
       }
dataset = MSSQLIterableDataset(**args)
dataloader = DataLoader(dataset, batch_size=3000,
                        shuffle=False, drop_last=False,
                        collate_fn=dataset.mssqlcollate)
```

### More examples at [Example notebook](https://github.com/Greatfess/iterable_dataset_mssql/MSSQLIterableDataset_examples.ipynb)