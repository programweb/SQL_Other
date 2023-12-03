# SQL_Other

Just adding some more SQL files here.
&nbsp;

Below you can see SQL in Python creating a new table and inserting data (insert from a select)
&nbsp;

```Python
"""Contains the MvCitationListNew class."""

from common import Database, get_logger

class MvCitationListNew:
    """MvCitationListNew class for creating a mv_citation_list_new table and populating it with data."""

    database = None # Holds the reference to which database
    all_active = False
    id_list = []

    def __init__(self, database: Database, id_csv_list):
        """Prepare for database work."""

        id_csv_list = id_csv_list.strip(' \t\n\r')
        if id_csv_list == 'ALL_ACTIVE':
            self.all_active = True
        elif id_csv_list == '':
            raise Exception(f"Dataset ID list cannot be empty.")
        elif id_csv_list.find(',') == 0:
            raise Exception(f"Dataset ID list should not start with a comma.")
        else:
            self.id_list = [id.strip() for id in id_csv_list.split(',')]

        if len(self.id_list) == 0 and self.all_active == False:
            raise Exception(f"Getting an empty Dataset ID list.")

        self.database = database
        logger = get_logger()

    def create_new_table(self):
        """Create table_name_removed table."""

        create_mv_citation_list_new_table_sql = """
            DROP TABLE IF EXISTS `db.table_name_removed`;
            CREATE TABLE `db.table_name_removed` (
            `citation_list_id` int(11) NOT NULL AUTO_INCREMENT,
            `nid` int(11) NOT NULL,
            `underlying_nid` int(11) DEFAULT NULL,
            `component_id` int(11) NOT NULL,
            `location_id` int(11) NOT NULL,
            `cause_id` int(11) DEFAULT NULL,
            `rei_id` int(11) DEFAULT NULL,
            `covariate_id` int(11) DEFAULT NULL,
            `sequela_id` int(11) DEFAULT NULL,
            `indicator_id` int(11) DEFAULT NULL,
            `is_private` int(11) DEFAULT NULL,
            `row_count` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
            `date_inserted` datetime NOT NULL DEFAULT current_timestamp() COMMENT 'Timestamp indicating when the row was added to this table.',
            `last_updated` datetime NOT NULL DEFAULT current_timestamp() COMMENT 'The timestamp value of when this row was last edited.',
            `last_updated_by` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'unknown' COMMENT 'The user''s username who last edited this row.',
            `last_updated_action` varchar(6) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'INSERT' COMMENT 'Last action performed on this row.',
            PRIMARY KEY (`citation_list_id`,`location_id`),
            KEY `idx_mv_citation_list_component` (`component_id`),
            KEY `idx_mv_citation_list_location` (`location_id`),
            KEY `idx_mv_citation_list_component_cause` (`component_id`,`cause_id`),
            KEY `idx_mv_citation_list_component_rei` (`component_id`,`rei_id`),
            KEY `idx_mv_citation_list_component_covariate` (`component_id`,`covariate_id`),
            KEY `idx_mv_citation_list_component_sequela` (`component_id`,`sequela_id`),
            KEY `idx_mv_citation_list_component_location` (`component_id`,`location_id`),
            KEY `idx_mv_citation_list_component_location_cause` (`component_id`,`location_id`,`cause_id`),
            KEY `idx_mv_citation_list_component_location_rei` (`component_id`,`location_id`,`rei_id`),
            KEY `idx_mv_citation_list_component_location_covariate` (`component_id`,`location_id`,`covariate_id`),
            KEY `idx_mv_citation_list_component_location_sequela` (`component_id`,`location_id`,`sequela_id`),
            KEY `idx_mv_citation_list_component_indicator` (`component_id`,`indicator_id`),
            KEY `idx_mv_citation_list_component_location_indicator` (`component_id`,`location_id`,`indicator_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
            """

        self.database.execute( create_mv_citation_list_new_table_sql )

    def get_data_set_info(self):
        """Get Data Set information (eg dataset IDs and tables with data to copy) in the form of a list of dictionaries."""

        data_sets_info = []

        if self.all_active:

            get_dataset_info_sql = """
                SELECT ds.id, ds.active, ds.table_name, ds.jira
                FROM db.table_name_removed ds
                WHERE ds.active = 1
                AND ds.table_name IN (
                    SELECT ist.TABLE_NAME
                    FROM information_schema.tables AS ist
                    WHERE TABLE_SCHEMA = 'citations');
                """

            return self.database.query_all( get_dataset_info_sql )

        else:

            for id in self.id_list:
                row = self.database.query_one("SELECT table_name FROM db.table_name_removed WHERE id = %s", (id,))
                tbl = row['table_name']

                if not tbl:
                    raise Exception(f"Problem fetching table_name using data set ID: {id}")

                data_set = {}
                data_set['id'] = id
                data_set['table_name'] = tbl
                data_sets_info.append(data_set)

        return data_sets_info


    def populate_new_table(self, table):
        """Add data to table_name_removed table from active citations"""

        pop_new_table_sql = """
            INSERT INTO db.table_name_removed( nid, underlying_nid,
                component_id, location_id, cause_id, rei_id, covariate_id,
                sequela_id, indicator_id, is_private, row_count, date_inserted,
                last_updated, last_updated_by, last_updated_action)
            SELECT nid, underlying_nid,
                component_id, location_id, cause_id, rei_id, covariate_id,
                sequela_id, indicator_id, is_private, row_count, date_inserted,
                last_updated, last_updated_by, last_updated_action
            FROM db_name_removed.{table};
            """

        self.database.execute( pop_new_table_sql.format(table=table) )

    def log_metadata(self, workticket, data_set_ids):

        log_sql = """
            INSERT INTO db.table_name_removed(active, table_name, date_created, jira, data_set_ids)
            VALUES(0, 'table_name_removed', NOW(), %s, %s)
            """

        self.database.execute(log_sql, (workticket, data_set_ids,))
```
