from transformer.transformer_with_past import Transformer
from pg_loader.pg_loader import PgLoader

if __name__ == "__main__":
    print("LOAD")
    loader = PgLoader()
    loader.print()
    fields = loader.transform_into_one()
    print("TRANSFORM")
    transformer = Transformer()
    transformer.prepare_data(fields)
