import boto3

def test_upload():
    s3 = boto3.client('s3')

    s3.put_object(
        Bucket='jad-data-pipeline',
        Key='test.txt',
        Body='Hello from my pipeline'
    )

    print("Upload successful")

if __name__ == "__main__":
    test_upload()