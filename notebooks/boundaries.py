import requests
import shapely
import shapely.ops
import shapely.geometry
import pandas as pd
import urllib3

# Disable SSL warnings when verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 

def get_boundaries(attr, val):
    """
    This function queries World Bank official boundary dataset via ArcGIS REST API
    and returns country boundary with geometry based on attribute and value.
    
    Input:
    attr: One of the attributes of database (e.g., 'ISO_A3', 'NAME_EN', 'WB_A3')
    val : Value corresponding to the attribute (e.g., 'HTI', 'Haiti')
    
    Common attributes:
    - ISO_A3: ISO 3-letter country code (e.g., 'HTI')
    - NAME_EN: Country name in English (e.g., 'Haiti')
    - WB_A2: World Bank 2-letter code
    - WB_A3: World Bank 3-letter code
    
    API Documentation: 
    https://services.arcgis.com/iQ1dY19aHwbSDYIF/ArcGIS/rest/services/World_Bank_Official_Boundaries_World_Country_Polygons_(Very_High_Definition)/FeatureServer/0
    
    Returns:
    A pandas DataFrame with features and geometry (shapely polygon) returned by the API.
    """
    
    url = ''.join([
        'https://services.arcgis.com/iQ1dY19aHwbSDYIF/ArcGIS/rest/services/',
        'World_Bank_Official_Boundaries_World_Country_Polygons_(Very_High_Definition)/FeatureServer/0/query?',
        f"where={attr}='{val}'&f=pjson&returnGeometry=true&outFields=*&outSR=4326"
    ])
    
    try:
        result = requests.get(url, timeout=30, verify=False)
        result.raise_for_status()
        
        res = result.json()
        
        if 'features' not in res or len(res['features']) == 0:
            print(f"No features found for {attr}='{val}'")
            return None
            
        # Extract attributes
        df = pd.DataFrame.from_dict(res['features'][0]).T[:-1]
        
        # Process geometry: combine all rings into a single multipolygon
        geom_list = []
        for i in range(len(res['features'][0]['geometry']['rings'])):
            geom_list.append(shapely.geometry.Polygon(res['features'][0]['geometry']['rings'][i]))
        
        # Use unary_union (replacement for deprecated cascaded_union)
        geom = shapely.ops.unary_union(geom_list)
        df.loc['attributes', 'rings'] = geom
        
        return df
        
    except requests.exceptions.Timeout:
        print(f"Request timed out for {attr}='{val}'")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None


def get_admin1_boundaries(iso_code):
    """
    This function queries World Bank Global Administrative Divisions API for admin level 1 boundaries
    (provinces/departments) and returns all admin 1 subdivisions for a country.
    
    Input:
    iso_code: ISO 3-letter country code (e.g., 'HTI' for Haiti, 'MMR' for Myanmar)
    
    API Documentation:
    https://services.arcgis.com/iQ1dY19aHwbSDYIF/ArcGIS/rest/services/World_Bank_Global_Administrative_Divisions/FeatureServer/2
    
    Returns:
    A pandas DataFrame with all admin 1 subdivisions and their geometries (shapely polygons).
    Each row represents one admin 1 subdivision (province/department/state).
    Key columns: ISO_A3, NAM_0 (country), NAM_1 (admin 1 name), ADM1CD_c (code), geometry
    """
    
    # World Bank Global Administrative Divisions - Layer 2 (ADM1)
    url = ''.join([
        'https://services.arcgis.com/iQ1dY19aHwbSDYIF/ArcGIS/rest/services/',
        'World_Bank_Global_Administrative_Divisions/FeatureServer/2/query?',
        f"where=ISO_A3='{iso_code}'&f=pjson&returnGeometry=true&outFields=*&outSR=4326"
    ])
    
    try:
        result = requests.get(url, timeout=30, verify=False)
        result.raise_for_status()
        
        res = result.json()
        
        if 'features' not in res or len(res['features']) == 0:
            print(f"No admin 1 features found for ISO='{iso_code}'")
            return None
        
        # Process all features into a DataFrame
        rows = []
        for feature in res['features']:
            # Extract attributes
            attrs = feature['attributes']
            
            # Process geometry: combine all rings into a single polygon
            geom_list = []
            if 'rings' in feature['geometry']:
                for ring in feature['geometry']['rings']:
                    geom_list.append(shapely.geometry.Polygon(ring))
                
                # Use unary_union to combine all rings
                geom = shapely.ops.unary_union(geom_list)
            else:
                geom = None
            
            attrs['geometry'] = geom
            rows.append(attrs)
        
        df = pd.DataFrame(rows)
        print(f"Found {len(df)} admin 1 subdivisions for {iso_code}")
        
        return df
        
    except requests.exceptions.Timeout:
        print(f"Request timed out for ISO='{iso_code}'")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None


def get_admin2_boundaries(iso_code):
    """
    This function queries World Bank Global Administrative Divisions API for admin level 2 boundaries
    (districts/communes) and returns all admin 2 subdivisions for a country.
    
    Input:
    iso_code: ISO 3-letter country code (e.g., 'HTI' for Haiti, 'MMR' for Myanmar)
    
    API Documentation:
    https://services.arcgis.com/iQ1dY19aHwbSDYIF/ArcGIS/rest/services/World_Bank_Global_Administrative_Divisions/FeatureServer/3
    
    Returns:
    A pandas DataFrame with all admin 2 subdivisions and their geometries (shapely polygons).
    Each row represents one admin 2 subdivision (district/commune/county).
    Key columns: ISO_A3, NAM_0 (country), NAM_1 (admin 1 name), NAM_2 (admin 2 name), ADM2CD_c (code), geometry
    """
    
    # World Bank Global Administrative Divisions - Layer 3 (ADM2)
    url = ''.join([
        'https://services.arcgis.com/iQ1dY19aHwbSDYIF/ArcGIS/rest/services/',
        'World_Bank_Global_Administrative_Divisions/FeatureServer/3/query?',
        f"where=ISO_A3='{iso_code}'&f=pjson&returnGeometry=true&outFields=*&outSR=4326"
    ])
    
    try:
        result = requests.get(url, timeout=30, verify=False)
        result.raise_for_status()
        
        res = result.json()
        
        if 'features' not in res or len(res['features']) == 0:
            print(f"No admin 2 features found for ISO='{iso_code}'")
            return None
        
        # Process all features into a DataFrame
        rows = []
        for feature in res['features']:
            # Extract attributes
            attrs = feature['attributes']
            
            # Process geometry: combine all rings into a single polygon
            geom_list = []
            if 'rings' in feature['geometry']:
                for ring in feature['geometry']['rings']:
                    geom_list.append(shapely.geometry.Polygon(ring))
                
                # Use unary_union to combine all rings
                geom = shapely.ops.unary_union(geom_list)
            else:
                geom = None
            
            attrs['geometry'] = geom
            rows.append(attrs)
        
        df = pd.DataFrame(rows)
        print(f"Found {len(df)} admin 2 subdivisions for {iso_code}")
        
        return df
        
    except requests.exceptions.Timeout:
        print(f"Request timed out for ISO='{iso_code}'")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None