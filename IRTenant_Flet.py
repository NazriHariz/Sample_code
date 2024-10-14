import flet as ft
import polars as pl
import connectorx as cx
import os
import cv2
import glob
import time

from flet import View, AppBar, Text, ElevatedButton, MainAxisAlignment, CrossAxisAlignment, RouteChangeEvent, ViewPopEvent
from azure.storage.blob import BlobServiceClient, ContentSettings
from db_connector_v2 import get_conn
from ir_utils_v1 import get_tenant
from time import sleep

def get_outlet_list():
    FILEHEADER, URI, project_id, container_name, sas_token= get_conn("IR")
    query = f"SELECT  DISTINCT O.OUTLET_NAME \
                FROM OUTLET O \
                INNER JOIN DEPLOYMENT D \
                ON O.OUTLET_ID = D.OUTLET_ID \
                WHERE D.PROJECT_ID = 10 "

    df_outlet = cx.read_sql(URI, query, return_type='polars')

    return df_outlet

def get_category_list():
    FFILEHEADER, URI, project_id, container_name, sas_token = get_conn("IR")
    query = f" SELECT DISTINCT IM.model_alias \
                FROM IR_MODEL IM \
                INNER JOIN product_category PC \
                ON PC.IR_MODEL_ID = IM.IR_MODEL_ID "


    df_category = cx.read_sql(URI, query, return_type='polars')

    return df_category
    

def main(page: ft.Page):
    
    page.title = "IRTenant-Insert-Record"
    page.theme_mode = ft.ThemeMode.DARK
    page.window_min_width = '700'
    page.window_min_height = '700'
    page.window_center = True
    page.scroll='always'
    
    #Home    
    def route_change(route):
        page.views.clear()
        page.views.append(
            ft.View(
                "/",
                [
                    ft.AppBar(title=ft.Text("HOME", color="BLACK", weight=ft.FontWeight.W_900), bgcolor="BLUE"),
                    ft.ElevatedButton(text = "LOCAL TO IR", width=200, height=50, bgcolor="BLUE", on_click=lambda _: page.go("/localtoir")),
                    ft.Container(height=30),
                    ft.ElevatedButton(text = "OTHER TENANT TO IR", width=250, height=50, bgcolor="BLUE", on_click=lambda _: page.go("/tenanttoir")),
                ],
                vertical_alignment=MainAxisAlignment.CENTER,
                horizontal_alignment=CrossAxisAlignment.CENTER,
                spacing=26
            )
        )
        if page.route == "/localtoir":
            page.views.append(
                ft.View(
                    "/localtoir",
                    [
                        ft.AppBar(title=ft.Text("LOCAL TO IR", color="BLACK", weight=ft.FontWeight.W_900), bgcolor="BLUE"),
                        ft.Container(height=30),
                        ft.Row([choice_date,date_button,ft.Container(width=80),cat_name,ft.Container(width=80),out_name]),
                        ft.Container(height=10),
                        ft.Row(controls=[dir_btn]),img_dir_txt,
                        ft.Container(height=10),
                        ft.Row(controls=[btn_insert,progress_bar]),
                        ft.Container(height=10),
                        ft.Column(controls = [output_container,output_container2]),
                    ],
                )
            )
        if page.route == "/tenanttoir":
            page.views.append(
                ft.View(
                    "/tenanttoir",
                    [
                        ft.AppBar(title=ft.Text("OTHER TENANT TO IR", color="BLACK", weight=ft.FontWeight.W_900), bgcolor="BLUE"),
                        ft.Container(height=30),
                        ft.Row([choice_date, date_button, ft.Container(width=60),
                                cat_name,ft.Container(width=60),out_name]),
                        ft.Container(height=10),
                        ft.Row([cg_text, cg, ft.Container(width=30), trx_id_txt]),
                        ft.Container(height=10),
                        ft.Row(controls=[btn_insert,progress_bar]),
                        ft.Container(height=10),
                        ft.Column(controls = [output_container,output_container2]),
                    ],
                )
            )
        page.update()

    def view_pop(view):
        page.views.pop()
        top_view = page.views[-1]
        page.go(top_view.route)
    
    choice_date = ft.TextField(label='Target Date', value="2023-09-01", width=140, height=45, border_color="blue")
    
    def update_target_date(e):
        choice_date.value=(date_picker.value.strftime("%Y-%m-%d"))
        choice_date.update()

    
    date_picker = ft.DatePicker()
    page.overlay.append(date_picker)
 
    def date(e):
        date_picker.on_change = update_target_date
        date_picker.pick_date()

    date_button = ft.IconButton(
        icon=ft.icons.CALENDAR_MONTH,
        on_click=date
    )


    cg = ft.RadioGroup(content=ft.Row([
        # Label = text displayed, Value = id
        ft.Radio(label="MT", value="MT"),
        ft.Radio(label="DT", value="DT"),
        ft.Radio(label="DT-IC", value="DT-IC"),
        ft.Radio(label="HNB", value="HNB")
    ]))

    # Set default active radio
    cg.value = "MT"

    cg_text = ft.Text("From Tenant: ")
    df_category = get_category_list()
    option = []
    for i in enumerate(df_category["model_alias"]):
        items = ft.dropdown.Option(i[1])
        option.append(items)

    # Create the Dropdown with options directly listed
    cat_name = ft.Dropdown(
        label="Category Name",
        hint_text="Choose Category Name",
        width=350,
        options = option,
        autofocus=True,
        border_color="blue"
    )

    df_outlet = get_outlet_list()
    option2 = []
    for i in enumerate(df_outlet["OUTLET_NAME"]):
        items = ft.dropdown.Option(i[1])
        option2.append(items)


    out_name = ft.Dropdown(
        label="Outlet Name",
        hint_text="Choose Outlet Name",
        width=450,
        options=option2,
        autofocus=True,
        border_color="blue"
    )
    
    img_dir_txt = ft.TextField(label='Image Dir', value='/home/ir-nazri/Documents/', width=700, height=45, border_color="blue")
    def pick_dir_result(e: ft.FilePickerResultEvent):
        img_dir_txt.value = e.path if e.path else "/home/ir-nazri/Documents/"
        img_dir_txt.update()

    dir_picker = ft.FilePicker(on_result=pick_dir_result)
    page.overlay.append(dir_picker)

    dir_btn=ft.FilledTonalButton(text='Select Image Dir', icon=ft.icons.FOLDER_OUTLINED,
                         on_click=lambda _: dir_picker.get_directory_path())
    
    
    trx_id_txt = ft.TextField(label='Trx ID', value='', width=200, height=45, border_color="blue")
    
    output_text = ft.Text("INSERT SUCCESFULL", size=50, weight=ft.FontWeight.W_900)

    output_container = ft.Container()
    output_container2 = ft.Container()

    def main(e):
        
        image_folder = img_dir_txt.value
        category_name = cat_name.value

        target_date = choice_date.value
        target_tenant = 'IR'
        outlet_name = out_name.value

        trx_id_other = trx_id_txt.value
        tenant = cg.value
        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(tenant)

        #search outlet name
        # query1 = f"select outlet_name from outlet where outlet_name = '{outlet_name}'"
        # outlet = cx.read_sql(URI, query1, return_type="polars")
        # result = outlet.select("outlet_name").item()


        if category_name == "DEODORANTS & FRAGRANCES":
            alias = "DEO"
        elif category_name == "DRESSINGS" or category_name == "DRESSING":
            alias = "DRES"
        elif category_name == "FABRIC CLEANING & ENHANCERS" or cat_name == "FABRIC CLEANING":
            alias = "FAB"
        elif category_name == "FUNCTIONAL NUTRITION":
            alias = "BVG"
        elif category_name == "HAIR CARE" or cat_name == "HNB - HAIR":
            alias = "HR"
        elif category_name == "HOME & HYGIENE":
            alias = "HH"
        elif category_name == "ICE CREAM":
            alias = "IC"
        elif category_name == "SCRATCH COOKING AIDS":
            alias = "SCA"
        elif category_name == "SKIN CARE" or cat_name.value == "HNB - SCR":
            alias = "SCR"
        elif category_name == "SKIN CLEANSING" or cat_name.value == "HNB - SCL":
            alias = "SCL"
        elif category_name == "NUTRITIONS" or cat_name.value == "OTHER NUTRITION":
            alias = "SP"
        elif category_name == "OUTLET-BASED":
            alias = "OUTLET-BASED"

        # if result is None:
        #
        #     #new_outlet(outlet_name,target_tenant)
        #     outlet_id = get_outlet_id(outlet_name, target_tenant)
        #     create_deployment(target_tenant, outlet_id)
        #     deploy_id = get_deployment_id(target_tenant, outlet_id)
        #else:
        outlet_id = get_outlet_id(outlet_name, target_tenant)
        deploy_id = get_deployment_id(target_tenant, outlet_id)


        if page.route == "/localtoir":

            df = get_record_from_local(image_folder)
            progress_bar.value = 0.2
            page.update()
            df_new = add_image_dimensions(df)
            print(df_new)
            source_records = copy_blob(df_new, target_tenant)
            print(source_records)
            progress_bar.value = 0.4
            page.update()
            category_id = insert_ir_category(outlet_id, target_tenant, target_date, category_name, deploy_id)
            progress_bar.value = 0.6
            page.update()
            trx_id = insert_cc_photos(source_records, outlet_id, category_name, target_date, target_tenant)
            progress_bar.value = 0.8
            page.update()
            update_ft_cc_photos(trx_id, target_tenant, category_id, target_date)
            progress_bar.value = 1.0
            page.update()

        elif page.route == "/tenanttoir":

            df = get_record(tenant, trx_id_other)
            progress_bar.value = 0.2
            page.update()
            source_records = copy_blob_2(df, tenant, target_tenant)
            progress_bar.value = 0.4
            page.update()
            category_id = insert_ir_category(outlet_id, target_tenant, target_date, category_name, deploy_id)
            progress_bar.value = 0.6
            page.update()
            trx_id = insert_cc_photos(source_records, outlet_id, category_name, target_date, target_tenant)
            progress_bar.value = 0.8
            page.update()
            update_ft_cc_photos(trx_id, target_tenant, category_id, target_date)
            progress_bar.value = 1.0
            page.update()
                


        link_qv = ft.Text(f"https://ulm-ir.azurewebsites.net/QuickView/Pages/ViewIRResultV2.aspx?irID={trx_id}&ProjID={project_id}&Category={alias}", selectable = True)

        output_container.content = output_text
        output_container.update()

        output_container2.content = link_qv
        output_container2.update()

    def get_record_from_local(image_folder):
        # Define the extensions you're interested in
        extensions = ('*.jpg', '*.jpeg', '*.png', '*.bmp', '*.tiff')

        # Get the list of image filenames with the specified extensions in the folder
        img_list = []
        for ext in extensions:
            img_list.extend(glob.glob(f"{image_folder}/{ext}"))

        # Check if the list of images is empty
        if not img_list:
            # Handle empty image list case
            page.dialog = ft.AlertDialog(title=ft.Text("No images found in the specified folder"))
            return

        # Initialize an empty DataFrame
        df = pl.DataFrame()

        # Append each image filename to the DataFrame
        for img in img_list:
            pl_image = pl.DataFrame({'Filename': [img]})
            df = df.vstack(pl_image)

        # Optionally, check if DataFrame is still empty after the loop (in case all files failed to load)
        if df.is_empty():
            dialog = ft.AlertDialog(title=ft.Text("Image fail to load"))
            page.overlay.append(dialog)
            return

        return df
    
    def get_record(tenant, trx_id):
        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(tenant)

        try:
            query = f"SELECT Filename, img_w, img_h FROM FT_IR_CC_PHOTOS WHERE Trx_id = '{trx_id}' AND isvalid = 1"

            df = cx.read_sql(URI, query, return_type='polars')
            print(f"Tenant trx_id: {tenant}-{trx_id}")
            print(df)

            if df.is_empty():
                dialog = ft.AlertDialog(title=ft.Text("No image to clone. Please check your TRX_ID"))
                page.overlay.append(dialog)
                return

        except Exception as e:
           dialog = ft.AlertDialog(title=ft.Text("No image to clone. Please check your TRX_ID"))
           page.overlay.append(dialog)

        time.sleep(1)

        return df

    def add_image_dimensions(df):

        # Initialize lists to hold dimension data
        widths = []
        heights = []

        # Iterate through each filename in the DataFrame
        for filename in df["Filename"].to_list():
            image = cv2.imread(filename)
            if image is not None:
                height, width = image.shape[:2]
                widths.append(width)
                heights.append(height)
            else:
                # Append None or an appropriate placeholder if the image can't be loaded
                print(f"Failed to load image at {filename}")
                widths.append(None)
                heights.append(None)

        # Create new columns in the DataFrame for width and height
        df = df.with_columns(
            pl.Series("img_w", widths),
            pl.Series("img_h", heights)
        )


        return df


    def copy_blob(df_new, target_tenant):
        # Import other necessary libraries and ensure get_tenant function is defined
        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(target_tenant)

        print(df_new)
        blob_service_client = BlobServiceClient(account_url=f"https://alwaysepst.blob.core.windows.net",
                                                credential=sas_token)

        for filename in df_new["Filename"]:

            destination_blob_name = f"Projects/IRDTHyperSuper/IRManual/{os.path.basename(filename)}"

            container_client = blob_service_client.get_container_client(container_name)
            destination_blob_client = container_client.get_blob_client(destination_blob_name)

            try:
                with open(filename, "rb") as file:
                    content = file.read()

                content_settings = ContentSettings(content_type='image/jpeg')
                destination_blob_client.upload_blob(content, content_settings=content_settings, overwrite=True)
                df_new = df_new.with_columns(pl.col("Filename").map_elements(lambda Filename: destination_blob_name if Filename == filename else Filename))
                print("Upload to Blob Succesfull")

            except Exception as e:
                print(f"Failed to process {filename}: {e}")
                dialog = ft.AlertDialog(title=ft.Text("Image fail to upload"))
                page.overlay.append(dialog)


        return df_new

    def copy_blob_2(df,tenant,target_tenant):

        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(tenant)
        FILEHEADER, URI, project_id, container_name2, sas_token2 = get_conn(target_tenant)

        blob_service_client_1 = BlobServiceClient(account_url=f"https://alwaysepst.blob.core.windows.net", credential=sas_token)
        container_client_1 = blob_service_client_1.get_container_client(container_name)

        blob_service_client_IR = BlobServiceClient(account_url=f"https://alwaysepst.blob.core.windows.net", credential=sas_token2)

        #file_path = []
        # Iterate through the DataFrame and copy each blob
        for filename in df["Filename"]:
            
            # Get a reference to the source blob
            source_blob_client = container_client_1.get_blob_client(filename)

            # Download the content of the source blob
            content = source_blob_client.download_blob().readall()

            # Get a reference to the destination blob
            IR_container_client = blob_service_client_IR.get_container_client(container_name2)
            destination_blob_name = f"Projects/IRDTHyperSuper/IRManual/{os.path.basename(filename)}"
            destination_blob_client = IR_container_client.get_blob_client(destination_blob_name)

            try:
                # Set the content type explicitly to 'image/jpeg'
                content_settings = ContentSettings(content_type='image/jpeg')

                # Upload the content to the destination blob with specified content type
                destination_blob_client.upload_blob(content, content_settings=content_settings, overwrite=True)

                df = df.with_columns(pl.col("Filename").map_elements(lambda Filename: destination_blob_name if Filename == filename else Filename))
                print("Upload to Blob Succesfull")
            except Exception as e:
                print(f"Failed to process {filename}: {e}")
                dialog = ft.AlertDialog(title=ft.Text("Image fail to upload"))
                page.overlay.append(dialog)


        print(df)
        time.sleep(1)
        return df


    def get_outlet_id(outlet_name,target_tenant):
        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(target_tenant)

        print(outlet_name)
        print(target_tenant)
        query = f"SELECT Outlet_id FROM outlet WHERE outlet_name = '{outlet_name}' "

        outlet = cx.read_sql(URI, query, return_type="polars")
        outlet_id = outlet['Outlet_id'].head(1).item()
        print("OUTLET ID:" + str(outlet_id))

        return outlet_id

    def create_deployment(target_tenant,outlet_id):
        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(target_tenant)
        query = f"INSERT INTO Deployment (project_id, outlet_id, Service_type, User_id, Schedule_type, status, Planned_route_id) VALUES ({project_id},{outlet_id},'Supervisor','13','Route','Active','4270')"

        try:
            cx.read_sql(URI, query)
            print("deployment created succesfully")
        except Exception as e:
            print(f"Error: {e}")
            print("Failed to insert new deployment record at " + target_tenant)


    def get_deployment_id(target_tenant, outlet_id):

        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(target_tenant)
        query = f"select deploy_id from deployment where project_id = '{project_id}' and outlet_id = '{outlet_id}'"

        deploy_id = cx.read_sql(URI, query, return_type="polars")
        deploy_id = deploy_id["deploy_id"].head(1).item()

        print("Deploy id:" + str(deploy_id))

        return deploy_id

    def insert_ir_category(outlet_id,target_tenant,target_date,category_name,deploy_id):
        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(target_tenant)
        conn, CURSOR, project_id = get_tenant(target_tenant)

        if category_name == "DRESSING":
            prod_cat_name = "DRESSINGS"
        elif category_name == "NUTRITIONS":
            prod_cat_name = "NUTRITION"
        elif category_name == "FABRIC CLEANING & ENHANCERS":
            prod_cat_name = "FABRIC CLEANING"
        elif category_name == "NUTRITION":
            prod_cat_name = "NUTRITIONS"
        elif category_name == "HNB - DEO":
            prod_cat_name = "DEODORANTS & FRAGRANCES"
        elif category_name == "HNB - SCL":
            prod_cat_name = "SKIN CLEANSING"
        elif category_name == "HNB - HAIR":
            prod_cat_name = "HAIR CARE"
        elif category_name == "HNB - SCR":
            prod_cat_name = "SKIN CARE"
        else:
            prod_cat_name = category_name


        query = f"SELECT IR_MODEL_ID FROM IR_MODEL WHERE MODEL_ALIAS = '{category_name}'"
        model = cx.read_sql(URI, query, return_type="polars")
        model_id = model["IR_MODEL_ID"].head(1).item()

        query2 = f"select category_id from Product_Category where category_name = '{prod_cat_name}'"
        category = cx.read_sql(URI, query2, return_type="polars")
        category_id = category["category_id"].head(1).item()

        query3 = f"INSERT INTO FT_IR_Category_checks (project_id, outlet_id, User_id, service_type, trx_date, status, category_id, Product_category_name,IR_model_id,IR_model_alias,isValid,created_date,updated_date,Deploy_id) VALUES (%s, %s, '13', 'Supervisor', %s, 'Active', %s, %s,%s,%s,'1', %s, %s, %s)"
        values = (project_id, outlet_id, target_date, category_id, prod_cat_name, model_id, category_name, target_date, target_date, deploy_id)
        try:
            CURSOR.execute(query3, values)
            conn.commit()  # Commit the changes to the database

            print("Insert new IR Record successful at " + target_tenant)
        except Exception as e:
            print(f"Error: {e}")
            print("Failed to insert new IR Record at " + target_tenant)
            dialog = ft.AlertDialog(title=ft.Text("Failed to insert new IR Record at " + target_tenant))
            page.overlay.append(dialog)

        time.sleep(1)
        return category_id

    def insert_cc_photos(source_records,outlet_id,category_name,target_date, target_tenant):

        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(target_tenant)
        conn, CURSOR, project_id = get_tenant(target_tenant)
        
        if category_name == "DRESSING":
            prod_cat_name = "DRESSINGS"
        elif category_name == "NUTRITIONS":
            prod_cat_name = "NUTRITION"
        elif category_name == "FABRIC CLEANING & ENHANCERS":
            prod_cat_name = "FABRIC CLEANING"
        elif category_name == "NUTRITION":
            prod_cat_name = "NUTRITIONS"
        elif category_name == "HNB - DEO":
            prod_cat_name = "DEODORANTS & FRAGRANCES"
        elif category_name == "HNB - SCL":
            prod_cat_name = "SKIN CLEANSING"
        elif category_name == "HNB - HAIR":
            prod_cat_name = "HAIR CARE"
        elif category_name == "HNB - SCR":
            prod_cat_name = "SKIN CARE"
        else:
            prod_cat_name = category_name

        # Get trx_id from ir_category check
        query1 = f"SELECT id FROM FT_IR_Category_checks WHERE project_id = '{project_id}' AND outlet_id = '{outlet_id}' AND Product_category_name = '{prod_cat_name}' AND created_date = '{target_date}'"
        id = cx.read_sql(URI, query1, return_type="polars")
        trx_id_result = id["id"].head(1).item()

        if trx_id_result:
            trx_id_copy = trx_id_result
            print("trx_id:" + str(trx_id_copy))
        else:
            print(f"No matching trx_id found for project_id '{project_id}' and outlet_id '{outlet_id}' and Product_category_name = '{prod_cat_name}'. Cloning aborted.")
            dialog = ft.AlertDialog(title=ft.Text("TRX_ID not found"))
            page.overlay.append(dialog)

            
        # Add the trx_id and category to the DataFrame
        source_records = source_records.with_columns([
            pl.lit(prod_cat_name).alias('Photo_type'),
            pl.lit("111111").alias('Length').cast(pl.Int32) 
        ])

        new_column = ["Photo_type","Filename", "Length", "img_w", "img_h"]
        
        source_records = source_records.select(new_column)
        
        # Assuming the columns are the same in both tables, modify it accordingly
        columns_str = ', '.join(source_records.columns)

        # Prepare the placeholders for the parameterized query
        placeholders = ', '.join(['%s'] * (len(source_records.columns) + 1))

        # Convert DataFrame values to a list of tuples
        records_to_insert = [(trx_id_copy,) + tuple(row) for row in source_records.to_numpy()]

        print(records_to_insert)
        # # Construct the INSERT query
        query = f"INSERT INTO FT_IR_CC_PHOTOS (trx_id, {columns_str}) VALUES ({placeholders})"



        try:
            CURSOR.executemany(query, records_to_insert)
            conn.commit()  # Commit the changes to the database

            print("Insert CC Photos successful at " + target_tenant)
        except Exception as e:
            print(f"Error: {e}")
            print("Failed to Insert CC Photos at " + target_tenant)
            dialog = ft.AlertDialog(title=ft.Text("Failed to Insert CC Photos at " + target_tenant))
            page.overlay.append(dialog)

        time.sleep(1)
        return trx_id_copy

    def update_ft_cc_photos(trx_id, target_tenant, category_id, target_date):

        FILEHEADER, URI, project_id, container_name, sas_token = get_conn(target_tenant)
        conn, CURSOR, project_id = get_tenant(target_tenant)

        content = "image/jpeg"
        #update necessary column
        query2=f"UPDATE FT_IR_CC_PHOTOS SET CONTENT_TYPE = '{content}', PHOTO_TIME = '{target_date}', INFERENCE_CHECK = '1', CREATED_DATE = '{target_date}', CATEGORY_ID = '{category_id}' WHERE TRX_ID = {trx_id}"

        try:
            CURSOR.execute(query2)
            conn.commit()  # Commit the changes to the database

            print("Update successful at " + target_tenant)
        except Exception as e:
            print(f"Error: {e}")
            print("Failed to Update at " + target_tenant)
            dialog = ft.AlertDialog(title=ft.Text("Failed to Update at " + target_tenant))
            page.overlay.append(dialog)
            
        time.sleep(1)
            
    btn_insert = ft.FilledTonalButton(text='Insert', on_click=main)
    
    progress_bar = ft.ProgressBar(value=0,width=1000)
    page.update()
    
    
    
    
    page.on_route_change = route_change
    page.on_view_pop = view_pop
    page.go(page.route)


ft.app(target=main)

