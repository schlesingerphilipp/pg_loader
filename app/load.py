from pg_loader.pg_loader import PgLoader

if __name__ == "__main__":
    loader = PgLoader()
    loader.print()
    loader.transform_into_one()
