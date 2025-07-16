#extract.py

import os


def download_json_files(ctx,folder_url,local_download_dir="downloads"):
    os.makedirs(local_download_dir,exist_ok=True)


    folder = ctx.web.get_folder_by_server_relative_url(folder_url)
    files = folder.files
    ctx.load(files)
    ctx.execute_query()

    print(f"Found {len(files)} files in SharePoint folder.")

    for file in files:
        file_name = file.properties["Name"]


        if file_name.endswith(".json"):
            local_path = os.path.join(local_download_dir,file_name)
            with open(local_path,"wb") as local_file:
                file.download(local_file).execute_query()
                print(f"Downloaded: {file_name}")