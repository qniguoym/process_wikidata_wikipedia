# coding: utf-8
"""Script to process Wikipedia and Wikidata dumps and create a knowledge base (KB)
with specific parameters. Intermediate files are written to disk.

Running the full pipeline on a standard laptop, may take up to 13 hours of processing.
Use the -p, -d and -s options to speed up processing using the intermediate files
from a previous run.

For the Wikidata dump: get the latest-all.json.bz2 from https://dumps.wikimedia.org/wikidatawiki/entities/
For the Wikipedia dump: get enwiki-latest-pages-articles-multistream.xml.bz2
from https://dumps.wikimedia.org/enwiki/latest/

"""
from __future__ import unicode_literals

import spacy

import logging
import os

import wikipedia_processor as wp, wikidata_processor as wd
import wiki_io as io
from wiki_io import TRAINING_DATA_FILE, KB_FILE, ENTITY_DESCR_PATH, KB_MODEL_DIR, LOG_FORMAT
from wiki_io import ENTITY_FREQ_PATH, PRIOR_PROB_PATH, ENTITY_DEFS_PATH, ENTITY_ALIAS_PATH, ENTITY_PROPER_PATH

logger = logging.getLogger(__name__)


def main(
    wd_json,
    wp_xml,
    output_dir,
    model=None,
    max_per_alias=10,
    min_freq=20,
    min_pair=5,
    entity_vector_length=64,
    loc_prior_prob=None,
    loc_entity_defs=None,
    loc_entity_alias=None,
    loc_entity_desc=None,
    descr_from_wp=False,
    limit_prior=None,
    limit_train=None,
    limit_wd=None,
    lang=None,
):
    entity_defs_path = os.path.join(output_dir,ENTITY_DEFS_PATH) #"entity_defs.csv"
    entity_alias_path = os.path.join(output_dir,ENTITY_ALIAS_PATH) #"entity_alias.csv"
    entity_descr_path = os.path.join(output_dir,ENTITY_DESCR_PATH) #"entity_descriptions.csv"
    entity_freq_path = os.path.join(output_dir,ENTITY_FREQ_PATH) #"entity_freq.csv"
    entity_proper_path = os.path.join(output_dir, ENTITY_PROPER_PATH)
    prior_prob_path = os.path.join(output_dir,PRIOR_PROB_PATH) #"prior_prob.csv"
    # training_entities_path = os.path.join(output_dir,TRAINING_DATA_FILE) #"gold_entities.jsonl"
    kb_path = os.path.join(output_dir,KB_FILE) #kb

    logger.info("Creating KB with Wikipedia and WikiData")

    # STEP 0: set up IO
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # STEP 1: Load the NLP object
    if model is not None:
        logger.info("STEP 1: Loading NLP model {}".format(model))
        nlp = spacy.load(model)

    # check the length of the nlp vectors
        if "vectors" not in nlp.meta or not nlp.vocab.vectors.size:
            raise ValueError(
                "The `nlp` object should have access to pretrained word vectors, "
                " cf. https://spacy.io/usage/models#languages."
            )

    # STEP 5: Getting gold entities from Wikipedia
    logger.info("STEP 5: Parsing and writing Wikipedia gold entities to {}".format(output_dir))
    if limit_train is not None:
        logger.warning("Warning: reading only {} lines of Wikipedia dump".format(limit_train))
    wp.create_training(wp_xml, entity_defs_path, output_dir, limit_train)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    wd_json = './data/wikidata-20210301-all.json.bz2'
    wp_xml = []
    output_dir = './data/output'
    lang_list = ['ja', 'de', 'es', 'ar', 'sr', 'tr', 'fa', 'ta', 'en',
                 'fr', 'it']
    # lang_list = ['ja','de']
    for lang in lang_list:
        path = './data/%swiki-20210301-pages-articles-multistream.xml.bz2'%lang
        wp_xml.append(path)

    main(wd_json,wp_xml,output_dir)
