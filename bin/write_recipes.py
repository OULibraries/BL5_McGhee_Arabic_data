#!/usr/bin/env python

# A quick Python 2 script for making recipe files
# because we're running it with the default CentOS 7 Python

import json 
import glob
import os
import uuid
import urllib2 
import hashlib
import ConfigParser
import boto3

from pprint import pprint
from lxml import etree


# Figure out where our files are and wire up config
#
script_path = os.path.dirname(os.path.abspath(__file__)) 
base = os.path.dirname(script_path)

my_config =ConfigParser.SafeConfigParser()
my_config.read( "%s/etc/config.ini" % base )
bag_location = my_config.get("main", "bag_location")
metadata_path = "%s/metadata/" % base
tn_path = base
recipe_path = "%s/recipes" % base

# Calculate repository uuid namespace and make sure that 
# we're doing the math right.
repo_uuid = uuid.uuid5( uuid.NAMESPACE_DNS, 'repository.ou.edu')
assert( "eb0ecf41-a457-5220-893a-08b7604b7110" == str(repo_uuid))

# Make recipes for all of our MODS files
print('processing %s/*.xml' % metadata_path)
mods_files = glob.glob('%s/*.xml' % metadata_path)
for mf in mods_files:

  # Get basename of item from metadata xml file. For book 
  # collections, this should map to a bag name. 
  mf_filename = os.path.basename(mf)
  bag_name = mf_filename[0:-4]
  item_uuid = uuid.uuid5(repo_uuid, bag_name)
  
  # Get item title from MODS
  tree = etree.parse(mf)
  title_tree = tree.xpath( '//mods:titleInfo/mods:title',
		       namespaces = {"mods":"http://www.loc.gov/mods/v3"})
  title = title_tree[0].text

  # Construct body of recipe
  recipe = {
    "import" : "book",
    "update" : "true",
    "uuid"   : str(item_uuid),
    "label"  : title,
    "metadata" : { 
      "mods" : mf 
    },
    "pages"  : []
  }

  # we'll be looping through items and pulling metadata from s3 for each
  s3 = boto3.client('s3')

  manifest = urllib2.urlopen(bag_location+"/source/"+bag_name+"/manifest-md5.txt")
  for line in manifest:
    tif_hash,page_path =  line.rstrip().split("  ")

    # We don't have to worry about .tiff spelling or other file types
    if not page_path.endswith("tif"):
      print( "...skipping "+page_path)
      continue

    # slicing from "data/page_slug.tif"
    page_slug=page_path[5:-4]
    page_jpg= "%s/derivative/%s/jpeg_040_antialias/%s.jpg" % (bag_location, bag_name, page_slug)
  

    # Use object ETag as MD5 hash. It will be in most cases. 
    s3_meta = s3.list_objects(Bucket="ul-bagit", 
                              Prefix="derivative/%s/jpeg_040_antialias/%s.jpg" % (bag_name, page_slug), 
                              MaxKeys=1)
    # remove first and last chars, string delivered quoted. 
    jpg_hash = s3_meta['Contents'][0]['ETag'][1:-1]

    page = {
      "uuid base": "%s/%s" % (bag_name, page_path),
      "uuid": str(uuid.uuid5(repo_uuid, "%s/%s" % (bag_name, page_path))),
      "sort": page_slug,
      "label": "Image %s"% page_slug.lstrip("0"),
      "file": page_jpg,
      "md5": jpg_hash,
    }
    recipe["pages"].append(page)
  
  pages = sorted( recipe["pages"], key= lambda d: d['sort'])

  recipe["pages"] = [{key : val for key, val in sub.items() if key not in ["sort", "uuid base"] } for sub in pages]

  print(" writing to %s/%s.json" %(recipe_path, bag_name))
  with open('%s/%s.json' %(recipe_path, bag_name), 'w') as f:
    json.dump( { "recipe" : recipe }, f, indent=2,)
