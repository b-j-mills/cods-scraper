### Collector for COD Datasets

This script collects COD service calls from the [ITOS API](https://apps.itos.uga.edu/CODV2API/api/v1/) and from the [ITOS geoservice directory](https://gistmaps.itos.uga.edu/arcgis/rest/services/) and creates corresponding resources on HDX. The scraper takes around 20 minutes to run. It makes in the order of 200 reads from the COD API and 1000 read/writes (API calls) to HDX in total. It does not create temporary files as it puts urls into HDX. It is run daily. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-cods** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE