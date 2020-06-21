from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import requests


scrape = 'https://www.99acres.com/search/project/buy/residential/delhi-ncr-all?search_type=QS&refSection=GNB&search_location=CP1&lstAcn=CP_R&lstAcnId=1&src=CLUSTER&preference=S&selected_tab=3&city=1&res_com=R&isvoicesearch=N&keyword_suggest=delhi%20%2F%20ncr%20(all)%3B&fullSelectedSuggestions=delhi%20%2F%20ncr%20(all)&strEntityMap=W3sidHlwZSI6ImNpdHkifSx7IjEiOlsiZGVsaGkgLyBuY3IgKGFsbCkiLCJDSVRZXzEsIFBSRUZFUkVOQ0VfUywgUkVTQ09NX1IiXX1d&texttypedtillsuggestion=delhi&refine_results=Y&Refine_Localities=Refine%20Localities&action=%2Fdo%2Fquicksearch%2Fsearch&suggestion=CITY_1%2C%20PREFERENCE_S%2C%20RESCOM_R&searchform=1&price_min=null&price_max=null'
property_links = []
project_data_rows = []
image_data_rows = []
floor_plan_data_rows = []
construction_data_rows = []
amenity_data_rows = []

driver_location = 'gecko_driver/chromedriver'
driver = webdriver.Chrome(driver_location)
driver.get(scrape)
driver.implicitly_wait(5)
soup = BeautifulSoup(driver.page_source, 'lxml')
soup = BeautifulSoup(soup.prettify(), 'lxml')
driver.quit()
project_links = soup.find_all('a', class_='npt_titl_desc')

for link in project_links:
    property_links.append(link['href'])

for link in property_links:
    driver = webdriver.Chrome(driver_location)
    driver.get(link)
    driver.implicitly_wait(5)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    soup = BeautifulSoup(soup.prettify(), 'lxml')
    project_data_row = {}
    if soup.find('div', class_='project-name') is not None:
        project_name = soup.find('div', class_='project-name').text.strip()
        project_data_row.__setitem__('Project Name', soup.find('div', class_='project-name').text.strip())
    if soup.find('div', class_='project-location fwn') is not None:
        project_data_row.__setitem__('Address', soup.find('div', class_='project-location fwn').span.text.strip())
    if soup.find('div', class_='factValsecond') is not None:
        project_data_row.__setitem__('New Booking Base Price', soup.find('div', class_='factValsecond').text.strip())
    if soup.find('input', id='mapWGT_LATITUDE') is not None:
        if soup.find('input', id='mapWGT_LATITUDE').get('value'):
            project_data_row.__setitem__('Latitude', soup.find('input', id='mapWGT_LATITUDE')['value'])
    if soup.find('input', id='mapWGT_LONGITUDE') is not None:
        if soup.find('input', id='mapWGT_LONGITUDE').get('value'):
            project_data_row.__setitem__('Longitude', soup.find('input', id='mapWGT_LONGITUDE')['value'])
    if soup.find('span', id='item_manufacturer') is not None:
        project_data_row.__setitem__('Builder', soup.find('span', id='item_manufacturer').text.strip())
    if len(soup.find_all('div', class_='factBox secFactTable')) != 0:
        for feature in soup.find_all('div', class_='factBox secFactTable'):
            if feature.text.find(':') != -1:
                project_data_row.__setitem__(feature.text.split(':')[0].strip(), feature.text.split(':')[1].strip())
    if len(soup.find_all('div', class_='factData')) != 0:
        for detail in soup.find_all('div', class_='factData'):
            if detail.find('div', class_='factLbl') is not None:
                if detail.find('div', class_='factVal2') is not None:
                    project_data_row.__setitem__(detail.find('div', class_='factLbl').text.strip()
                                                 , '{}||{}'.format(detail.find('div', class_='factVal1').text.strip()
                                                                   .split('\n')[0], detail.find('div', class_='factVal2')
                                                                   .text.strip().split('\n')[0]))
                else:
                    project_data_row.__setitem__(detail.find('div', class_='factLbl').text.strip()
                                                 , detail.find('div', class_='factVal1').text.strip())
    project_data_rows.append(project_data_row)

    # images

    if len(soup.find_all('li', class_='xid_galleryItem dev_imageTuple')) != 0:
        images = soup.find_all('li', class_='xid_galleryItem dev_imageTuple')
        for image in images:
            image_data_row = {}
            if soup.find('div', class_='project-name') is not None:
                image_data_row.__setitem__('Project Name', soup.find('div', class_='project-name').text.strip())
            if image.img.get('data-original'):
                image_data_row.__setitem__('link', image.img['data-original'])
            else:
                image_data_row.__setitem__('link', image.img['src'])
            image_data_row.__setitem__('image_title', image.text.strip())
            image_data_rows.append(image_data_row)
    elif len(soup.find_all('a', class_='dev_tupleClick')) != 0:
        images = soup.find_all('a', class_='dev_tupleClick')
        for image in images:
            image_data_row = {}
            if soup.find('div', class_='project-name') is not None:
                image_data_row.__setitem__('Project Name', soup.find('div', class_='project-name').text.strip())
            if image.img.get('src'):
                image_data_row.__setitem__('link', image.img['src'])
            if image.get('data-entity'):
                image_data_row.__setitem__('image_title', image['data-entity'])
            image_data_rows.append(image_data_row)

    # Floor Plans

    floor_plans = soup.find_all('div', class_='fpcRow flt flex qaOptionTuple dev_optionTuple')
    for floor_plan in floor_plans:
        floor_plan_row = {}
        if soup.find('div', class_='project-name') is not None:
            floor_plan_row.__setitem__('Project Name', soup.find('div', class_='project-name').text.strip())
        if floor_plan.get('data-property-type'):
            floor_plan_row.__setitem__('Property Type', floor_plan['data-property-type'])
        if floor_plan.img.get('data-orig_img'):
            floor_plan_row.__setitem__('Floor Plan Image Link', floor_plan.img['data-orig_img'])
        if floor_plan.find('div', class_='fpcColumn width17per flt qaInclusions') is not None:
            inclusions = floor_plan.find('div', class_='fpcColumn width17per flt qaInclusions').text.split('\n')
            inclusions = [inclusion.strip() for inclusion in inclusions if len(inclusion.strip()) > 0]
            floor_plan_row.__setitem__('Inclusions', '||'.join(inclusions))
        if floor_plan.find('div', class_='fpcColumn width30per flt qaAreaDiv') is not None:
            area_details = floor_plan.find('div', class_='fpcColumn width30per flt qaAreaDiv')
            if len(area_details.findAll('div', class_='textContainer')) != 0:
                area_details = area_details.findAll('div', class_='textContainer')
                area_details = ['{}: {} Sqft'.format(area.span.text.strip(), area.em.text.strip())
                                for area in area_details]
                floor_plan_row.__setitem__('Area Details', '||'.join(area_details))
        if floor_plan.find('div', class_='fpcColumn width36per buttonAlign flt qaNewBookingPriceDiv') is not None:
            if floor_plan.find('div', class_='fpcColumn width36per buttonAlign flt qaNewBookingPriceDiv').span is not None:
                floor_plan_row.__setitem__('New Booking Base Price', floor_plan.
                                           find('div', class_='fpcColumn width36per buttonAlign flt qaNewBookingPriceDiv')
                                           .span.text.strip())
        floor_plan_data_rows.append(floor_plan_row)

    # Construction

    if len(soup.find_all('tr', class_='mt10 rel pb35')) != 0:
        constructions = soup.find_all('tr', class_='mt10 rel pb35')
        for construction in constructions:
            construction_data_row = {}
            if soup.find('div', class_='project-name') is not None:
                construction_data_row.__setitem__('Project Name', soup.find('div', class_='project-name').text.strip())
            if construction.find('div', class_='reraBandSite') is not None:
                rera_website = construction.find('div', class_='reraBandSite').text.strip()
                if rera_website.find('http') != -1:
                    construction_data_row.__setitem__('Rera Website', rera_website[rera_website.find('http'):])
            if construction.find('span', class_='xid_constPhaseLabel') is not None:
                construction_data_row.__setitem__('Phase', construction.find('span', class_='xid_constPhaseLabel')
                                                  .text.strip())
            if construction.find('span', class_='xid_constPhaseStatusLevel') is not None:
                construction_data_row.__setitem__('Phase Status', construction
                                                  .find('span', class_='xid_constPhaseStatusLevel').text.strip())
            if construction.find('span', class_='xid_constPhaseStatusDate') is not None:
                construction_data_row.__setitem__('Phase Completion Date', construction
                                                  .find('span', class_='xid_constPhaseStatusDate').text.strip())
            if construction.find('div', class_='xid_constPhasesText') is not None:
                construction_data_row.__setitem__('Phase Deliverables', construction
                                                  .find('div', class_='xid_constPhasesText').text.strip())
            if construction.find('div', class_='xid_constPhasesText textOverflow') is not None:
                construction_data_row.__setitem__('Phase Units', construction
                                                  .find('div', class_='xid_constPhasesText textOverflow').text.strip())
            if construction.find('span', class_='xid_constPhasesPrice') is not None:
                construction_data_row.__setitem__('Booking Price', construction
                                                  .find('span', class_='xid_constPhasesPrice').text.strip())
            if construction.find('div', class_='reraInfoTxt reraTextColor') is not None:
                construction_data_row.__setitem__('Rera_Status', construction.
                                                  find('div', class_='reraInfoTxt reraTextColor').text.strip())
            if construction.find('span', class_='elipsis') is not None:
                if construction.find('span', class_='elipsis').get('title'):
                    construction_data_row.__setitem__('Rera Registration Number', construction
                                                      .find('span', class_='elipsis')['title'].strip())
            construction_data_rows.append(construction_data_row)
    else:
        construction_data_row = {}
        if soup.find('div', class_='project-name') is not None:
            construction_data_row.__setitem__('Project Name', soup.find('div', class_='project-name').text.strip())
        if soup.find('div', class_='npReraSite') is not None:
            rera_website = soup.find('div', class_='npReraSite').text.strip()
            if rera_website.find('http') != -1:
                construction_data_row.__setitem__('Rera Website', rera_website[rera_website.find('http'):])
        if soup.find('div', class_='reraInfoTxt reraTextColor') is not None:
            construction_data_row.__setitem__('Rera_Status', soup.
                                              find('div', class_='reraInfoTxt reraTextColor').text.strip())
        if soup.find('div', class_='npReraText') is not None:
            construction_data_row.__setitem__('Rera Registration Number', soup
                                              .find('div', class_='npReraText').text.strip())
        construction_data_rows.append(construction_data_row)

    # amenities

    if soup.find('a', id='amenities_tab') is None:
        continue
    amenities_link = soup.find('a', id='amenities_tab')['href']
    if amenities_link is None:
        continue
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'
    }
    amenity_data = requests.get(amenities_link, headers=headers).content
    soup = BeautifulSoup(amenity_data, 'lxml')
    soup = BeautifulSoup(soup.prettify(), 'lxml')
    if len(soup.find_all('ul', class_='features_int_ext f12 m5')) == 0:
        continue
    for amenities in soup.find_all('ul', class_='features_int_ext f12 m5'):
        if len(amenities.find('li', class_='label_bg').text.strip()) == 0:
            amenity_category = 'Extra Features'
        else:
            if amenities.find('li', class_='label_bg') is not None:
                amenity_category = amenities.find('li', class_='label_bg').text.strip()
        if len(amenities.findAll('li')) == 0:
            continue
        for amenity in amenities.findAll('li'):
            amenity_data_row = {}
            amenity_data_row.__setitem__('Amenity Category', amenity_category)
            amenity_data_row.__setitem__('Project Name', project_name)
            if amenity.text.strip() in [amenity_category, '', '0']:
                continue
            else:
                amenity_data_row.__setitem__('Amenity', amenity.text.strip())
            amenity_data_rows.append(amenity_data_row)
    driver.quit()

pd.DataFrame(project_data_rows).to_csv('project_data.csv', index=False, encoding='utf-8-sig')
pd.DataFrame(image_data_rows).to_csv('project_images.csv', index=False, encoding='utf-8-sig')
pd.DataFrame(construction_data_rows).to_csv('project_constructions.csv', index=False, encoding='utf-8-sig')
pd.DataFrame(floor_plan_data_rows).to_csv('project_floor_plans.csv', index=False, encoding='utf-8-sig')
pd.DataFrame(amenity_data_rows).to_csv('project_amenities.csv', index=False, encoding='utf-8-sig')




