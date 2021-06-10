from app.transformer.transformer_from_csv import Transformer
from app.pg_loader.pg_loader import PgLoader
from app.etlconfig import docker, local

if __name__ == "__main__":
    config = docker() # local() for local config
    print("LOAD")
    #loader = PgLoader(config)
    #loader.print()
    #loader.transform_into_one()
    print("TRANSFORM")
    transformer = Transformer(config)
    transformer.prepare_data()
