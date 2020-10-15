# Python
Primary language set for handling.
## [Modules](../modules)
#### Production
- [__init__](../modules/__init__.py)
    - primary import source for project modules.
    - primary project directory source for project modules
- [sql.py](../modules/sql_engines.py)
    - original sqlite class created for data transfer/handling.
#### Development
- [wpengine-class.py](../modules/wpengine-class.py)
    - original attempt at connecting to the wpengine production site's mysql database.
    - May be replaced.
# SQL
The instance of WP Engine that hosts The Pitt runs on 
MySql version 5.7. CTE syntax is not introduced until
MySql version 8.X. This is another reason why a central
database for business intelligence is highly recommended.
If absolutely necessary, you can nest statements together.
# Docker
