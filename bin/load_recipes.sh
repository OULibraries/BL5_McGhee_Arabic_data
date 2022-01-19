#!/bin/bash
if [ $# -ne 3 ]; then
  echo "Incorrect number of arguments given. Usage is load_recipes.sh \$DRUPAL_ROOT \$IFIRS_DATA \$TMP_DIR"
  exit
fi


for recipe in `ls recipes/*.json`
do
  echo drush --root="${1}"  --user=1 oubib --recipe_uri="${2}/${recipe}" --parent_collection=oku:ifirs --tmp_dir=${3} --pid_namespace=oku
done
