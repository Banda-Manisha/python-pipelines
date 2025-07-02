from extract import connect_to_mongodb,extract_documentt
from load import connect_to_dynamodb,load_to_dynamodb


def main():
    print('connecting to mongo')
    con = connect_to_mongodb()

    print("extraction started")
    raw_df = extract_documentt()

    print("connecting to dynamodb")
    conn = connect_to_dynamodb()

    print(" loading started")
    load_df = load_to_dynamodb(raw_df,conn)

if __name__ == "__main__":
    main()


