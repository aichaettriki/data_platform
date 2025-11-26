from apache_atlas.client import AtlasClient
from apache_atlas.model.instance import AtlasEntity

def push_dag_to_atlas(dag_id, description=""):
    atlas = AtlasClient('http://atlas:21000', ('admin', 'admin'))
    entity = AtlasEntity(
        name=dag_id,
        typeName='Process',
        attributes={
            'qualifiedName': f'{dag_id}@my_domain',
            'name': dag_id,
            'description': description
        }
    )
    atlas.create_entity(entity)
