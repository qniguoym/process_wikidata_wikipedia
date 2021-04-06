# coding: utf-8
from __future__ import unicode_literals

import bz2
import json
import logging

from wiki_namespaces import WD_META_ITEMS

logger = logging.getLogger(__name__)


def read_wikidata_entities_json(
    wikidata_file, limit=None, to_print=False, lang=None, parse_descr=True
):
    # Read the JSON wiki data and parse out the entities. Takes about 7-10h to parse 55M lines.
    # get latest-all.json.bz2 from https://dumps.wikimedia.org/wikidatawiki/entities/
    if lang is None:
        lang= ['ja', 'de', 'es', 'ar', 'sr', 'tr', 'fa', 'ta', 'en',
                 'fr', 'it']

    # site_filter = "{}wiki".format(lang)

    # filter: currently defined as OR: one hit suffices to be removed from further processing
    exclude_list = WD_META_ITEMS

    # punctuation
    exclude_list.extend(["Q1383557", "Q10617810"])

    # letters etc
    exclude_list.extend(
        ["Q188725", "Q19776628", "Q3841820", "Q17907810", "Q9788", "Q9398093"]
    )

    ###论文中提到的一些list
    exclude_list.extend(['Q4167836','Q24046192','Q20010800','Q11266439','Q11753321',
                'Q19842659','Q21528878','Q17362920','Q14204246','Q21025364','Q17442446',
                'Q26267864','Q4663903','Q15184295'])

    neg_prop_filter = {
        "P31": exclude_list,  # instance of
        "P279": exclude_list,  # subclass
    }

    title_to_id = dict()
    id_to_descr = dict()
    id_to_alias = dict()
    id_to_proper = dict()

    # parse appropriate fields - depending on what we need in the KB
    parse_properties = True
    parse_sitelinks = True ###每个语言对应的形式
    parse_labels = False
    parse_aliases = True
    parse_claims = True ####过滤一些数据

    with bz2.open(wikidata_file, mode="rb") as file:
        for cnt, line in enumerate(file):
            if limit and cnt >= limit:
                break
            if cnt % 500000 == 0 and cnt > 0:
                logger.info("processed {} lines of WikiData JSON dump".format(cnt))
            clean_line = line.strip()
            if clean_line.endswith(b","):
                clean_line = clean_line[:-1]
            if len(clean_line) > 1:
                try:
                    obj = json.loads(clean_line)
                except:
                    continue
                entry_type = obj["type"]

                if entry_type == "item" and len(obj['descriptions'])>0:
                    keep = True

                    claims = obj["claims"]
                    if parse_claims:
                        for prop, value_set in neg_prop_filter.items():
                            claim_property = claims.get(prop, None)
                            if claim_property:
                                for cp in claim_property:
                                    cp_id = (
                                        cp["mainsnak"]
                                        .get("datavalue", {})
                                        .get("value", {})
                                        .get("id")
                                    )
                                    cp_rank = cp["rank"]
                                    if cp_rank != "deprecated" and cp_id in value_set:
                                        keep = False
                                        break
                            if not keep:
                                break

                    if keep:
                        unique_id = obj["id"]

                        if to_print:
                            print("ID:", unique_id)
                            print("type:", entry_type)

                        # parsing all properties that refer to other entities
                        if parse_properties:
                            proper_list = id_to_proper.get(unique_id, [])
                            for prop, claim_property in claims.items():
                                cp_dicts = [
                                    cp["mainsnak"]["datavalue"].get("value")
                                    for cp in claim_property
                                    if cp["mainsnak"].get("datavalue")
                                ]
                                cp_values = [
                                    cp_dict.get("id")
                                    for cp_dict in cp_dicts
                                    if isinstance(cp_dict, dict)
                                    if cp_dict.get("id") is not None
                                ]
                                if cp_values:
                                    if to_print:
                                        print("prop:", prop, cp_values)

                                    proper_list.append((prop,cp_values))
                            id_to_proper[unique_id] = proper_list

                        if parse_sitelinks:
                            if isinstance(lang,list):
                                for l in lang:
                                    site_filter = "{}wiki".format(l)
                                    site_value = obj["sitelinks"].get(site_filter, None)
                                    if site_value:
                                        site = site_value["title"]
                                        if to_print:
                                            print(site_filter, ":", site)
                                        # if site_filter+'_'+site in title_to_id:
                                        #     if unique_id!=title_to_id[site_filter+'_'+site]:
                                        #         print(site)
                                        #         print(unique_id)
                                        #         print(title_to_id[site_filter+'_'+site])
                                        title_to_id[l+'_'+site] = unique_id
                                        # if l == 'ar':
                                        #     print(site)
                                        #     print(unique_id)
                            else:
                                site_filter = "{}wiki".format(lang)
                                site_value = obj["sitelinks"].get(site_filter, None)
                                if site_value:
                                    site = site_value["title"]
                                    if to_print:
                                        print(site_filter, ":", site)
                                    title_to_id[site] = unique_id

                        if parse_labels:
                            labels = obj["labels"]
                            if labels:
                                lang_label = labels.get(lang, None)
                                if lang_label:
                                    if to_print:
                                        print(
                                            "label (" + lang + "):", lang_label["value"]
                                        )

                        if parse_descr:
                            descriptions = obj["descriptions"]
                            if descriptions:
                                if isinstance(lang,list):
                                    des_tmp = {}
                                    for l in lang:
                                        lang_descr = descriptions.get(l, None)
                                        if lang_descr:
                                            des_tmp[l]=lang_descr["value"]
                                    id_to_descr[unique_id]=des_tmp
                                else:
                                    lang_descr = descriptions.get(lang, None)
                                    if lang_descr:
                                        if to_print:
                                            print(
                                                "description (" + lang + "):",
                                                lang_descr["value"],
                                            )
                                        id_to_descr[unique_id] = lang_descr["value"]

                        if parse_aliases:
                            aliases = obj["aliases"]

                            if aliases:
                                if isinstance(lang,list):
                                    alias_list = {}
                                    for l in lang:
                                        lang_aliases = aliases.get(l, None)
                                        if lang_aliases:
                                            alias_list[l] = []
                                            for item in lang_aliases:
                                                alias_list[l].append(item["value"])
                                    id_to_alias[unique_id] = alias_list
                                else:
                                    lang_aliases = aliases.get(lang, None)
                                    if lang_aliases:
                                        for item in lang_aliases:
                                            if to_print:
                                                print(
                                                    "alias (" + lang + "):", item["value"]
                                                )
                                            alias_list = id_to_alias.get(unique_id, [])
                                            alias_list.append(item["value"])
                                            id_to_alias[unique_id] = alias_list

                        if to_print:
                            print()

    # log final number of lines processed
    logger.info("Finished. Processed {} lines of WikiData JSON dump".format(cnt))
    return title_to_id, id_to_descr, id_to_alias, id_to_proper
