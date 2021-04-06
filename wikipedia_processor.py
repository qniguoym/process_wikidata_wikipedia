# coding: utf-8
from __future__ import unicode_literals
import pickle
import re
import bz2
import logging
import random
import json
from polyglot.text import Text
import wiki_io as io
from wiki_namespaces import WP_META_NAMESPACE, WP_FILE_NAMESPACE, WP_CATEGORY_NAMESPACE
import os

"""
Process a Wikipedia dump to calculate entity frequencies and prior probabilities in combination with certain mentions.
Write these results to file for downstream KB and training data generation.

Process Wikipedia interlinks to generate a training dataset for the EL algorithm.
"""

ENTITY_FILE = "gold_entities.csv"

map_alias_to_link = dict()

logger = logging.getLogger(__name__)

title_regex = re.compile(r"(?<=<title>).*(?=</title>)")
id_regex = re.compile(r"(?<=<id>)\d*(?=</id>)")
text_tag_regex = re.compile(r"(?<=<text).*?(?=>)")
text_regex = re.compile(r"(?<=<text>).*(?=</text)")
info_regex = re.compile(r"{[^{]*?}")
html_regex = re.compile(r"&lt;!--[^-]*--&gt;")
ref_regex = re.compile(r"&lt;ref.*?&gt;")  # non-greedy
ref_2_regex = re.compile(r"&lt;/ref.*?&gt;")  # non-greedy

# find the links
link_regex = re.compile(r"\[\[[^\[\]]*\]\]") #[^]所有不在集合范围内的词可以被匹配，*表示前面的一次或者多次匹配

# match on interwiki links, e.g. `en:` or `:fr:`
ns_regex = r":?" + "[a-z][a-z]" + ":" ###?是尽量少的字符串被匹配，0-1次
# match on Namespace: optionally preceded by a :
for ns in WP_META_NAMESPACE:
    ns_regex += "|" + ":?" + ns + ":"

ns_regex = re.compile(ns_regex, re.IGNORECASE)

files = r""
for f in WP_FILE_NAMESPACE:
    files += "\[\[" + f + ":[^\[\]]*(\[\[[^\[\]]*\]\])*[^\[\]]*\]\]" + "|"
files = files[0 : len(files) - 1]
file_regex = re.compile(files)

cats = r""
for c in WP_CATEGORY_NAMESPACE:
    cats += "\[\[" + c + ":[^\[\]]*(\[\[[^\[\]]*\]\])*[^\[\]]*\]\]" + "|"
cats = cats[0 : len(cats) - 1]
category_regex = re.compile(cats)

from others import *


def read_prior_probs(wikipedia_input_list, prior_prob_output, limit=None):

    cnt = 0
    read_id = False
    for wikipedia_input in wikipedia_input_list:
        print(wikipedia_input)
        lang = wikipedia_input[7:9]
        print(lang)
        with bz2.open(wikipedia_input, mode="rb") as file:
            line = file.readline()
            while line and (not limit or cnt < limit):
                if cnt % 25000000 == 0 and cnt > 0:
                    logger.info("processed {} lines of Wikipedia XML dump".format(cnt))
                clean_line = line.strip().decode("utf-8")

                # we attempt at reading the article's ID (but not the revision or contributor ID)
                if "<revision>" in clean_line or "<contributor>" in clean_line:
                    read_id = False
                if "<page>" in clean_line:
                    read_id = True

                if read_id:
                    ids = id_regex.search(clean_line)
                    if ids:
                        current_article_id = ids[0]

                # only processing prior probabilities from true training (non-dev) articles
                # if not is_dev(current_article_id):
                aliases, entities, normalizations = get_wp_links(clean_line)
                for alias, entity, norm in zip(aliases, entities, normalizations):
                    _store_alias(
                        alias, entity, lang, normalize_alias=norm, normalize_entity=True)

                line = file.readline()
                cnt += 1

            logger.info("processed {} lines of Wikipedia XML dump".format(cnt))

    logger.info("Finished. processed {} lines of Wikipedia XML dump".format(cnt))

    # write all aliases and their entities and count occurrences to file
    with open(prior_prob_output,'w',encoding="utf8") as outputfile:
        outputfile.write("alias" + "|" + "count" + "|" + "entity" + "\n")
        for alias, alias_dict in sorted(map_alias_to_link.items(), key=lambda x: x[0]):
            ##alias->alias_dict entity:count，一个alias可能对应着多个实体，因此有多个count
            s_dict = sorted(alias_dict.items(), key=lambda x: x[1], reverse=True)
            for entity, count in s_dict:
                outputfile.write(alias + "|" + str(count) + "|" + entity + "\n")


def read_prior_probs_for_des(wikipedia_input_list, prior_prob_output, def_input, limit=None):

    cnt = 0
    read_id = False
    wp_to_id = io.read_title_to_id(def_input)
    # print('wp_to_id', len(wp_to_id)) #15608263
    # print(list(wp_to_id.keys()))
    # exit()

    record = {}
    lang_num={}
    for wikipedia_input in wikipedia_input_list:
        print(wikipedia_input)
        lang = wikipedia_input[7:9]
        print(lang)
        lang_num[lang]=0
        with bz2.open(wikipedia_input, mode="rb") as file:
            line = file.readline()
            while line and (not limit or cnt < limit):
                if cnt % 25000000 == 0 and cnt > 0:
                    logger.info("processed {} lines of Wikipedia XML dump".format(cnt))
                clean_line = line.strip().decode("utf-8")

                # we attempt at reading the article's ID (but not the revision or contributor ID)
                if "<revision>" in clean_line or "<contributor>" in clean_line:
                    read_id = False
                if "<page>" in clean_line:
                    read_id = True

                if read_id:
                    ids = id_regex.search(clean_line)
                    if ids:
                        current_article_id = ids[0]

                # only processing prior probabilities from true training (non-dev) articles
                # if not is_dev(current_article_id):
                aliases, entities, normalizations = get_wp_links(clean_line)
                for alias, entity, norm in zip(aliases, entities, normalizations):
                    alias = alias.strip()
                    entity = entity.strip()
                    entity = _capitalize_first(entity.split("#")[0])
                    if norm:
                        alias = alias.split("#")[0]

                    if alias and entity:
                        if lang+'_'+entity in wp_to_id:
                            id = wp_to_id[lang+'_'+entity]
                        else:
                            continue

                        if id not in record:
                            record[id]={}
                        if lang not in record[id]:
                            record[id][lang]=0

                        record[id][lang]+=1
                        lang_num[lang]+=1

                line = file.readline()
                cnt += 1

            logger.info("processed {} lines of Wikipedia XML dump".format(cnt))

    logger.info("Finished. processed {} lines of Wikipedia XML dump".format(cnt))

    # write all aliases and their entities and count occurrences to file
    with open(prior_prob_output,'wb') as outputfile:
        pickle.dump((record,lang_num),outputfile)


def _store_alias(alias, entity, lang, normalize_alias=False, normalize_entity=True):
    alias = alias.strip()
    entity = entity.strip()

    # remove everything after # as this is not part of the title but refers to a specific paragraph
    if normalize_entity:
        # wikipedia titles are always capitalized
        entity = _capitalize_first(entity.split("#")[0])
    if normalize_alias:
        alias = alias.split("#")[0]

    if alias and entity:
        entity = lang+'_'+entity
        alias_dict = map_alias_to_link.get(alias, dict())
        entity_count = alias_dict.get(entity, 0)
        alias_dict[entity] = entity_count + 1
        map_alias_to_link[alias] = alias_dict  ##alias->alias_dict entity:count，一个alias可能对应着多个实体，因此有多个count


def get_wp_links(text):
    aliases = []
    entities = []
    normalizations = []

    matches = link_regex.findall(text) ##[[]]框起来的是链接

    for match in matches:
        match = match[2:][:-2].replace("_", " ").strip()

        if ns_regex.match(match):
            pass  # ignore the entity if it points to a "meta" page or category page

        # this is a simple [[link]], with the alias the same as the mention
        elif "|" not in match:
            aliases.append(match)
            entities.append(match)
            normalizations.append(True)

        # in wiki format, the link is written as [[entity|alias]]
        else:
            splits = match.split("|")
            entity = splits[0].strip()
            alias = splits[1].strip()
            # specific wiki format  [[alias (specification)|]]
            if len(alias) == 0 and "(" in entity:
                alias = entity.split("(")[0]
                aliases.append(alias)
                entities.append(entity)
                normalizations.append(False)
            else:
                aliases.append(alias)
                entities.append(entity)
                normalizations.append(False)

    return aliases, entities, normalizations


def _capitalize_first(text):
    if not text:
        return None
    result = text[0].capitalize()
    if len(result) > 0:
        result += text[1:]
    return result


def create_training(
    wp_input, def_input, output_dir, limit=None
):
    wp_to_id = io.read_title_to_id(def_input)
    _process_wikipedia_texts(wp_input, wp_to_id, output_dir, limit)


def _process_wikipedia_texts(
    wikipedia_input_list, wp_to_id, output_dir, limit=None):
    """
    Read the XML wikipedia data to parse out training data:
    raw text data + positive instances
    """

    # read_ids = set()

    for wikipedia_input in wikipedia_input_list:
        lang = wikipedia_input.split('/')[-1][0:2]
        training_output = os.path.join(output_dir,'gold_entities_%s.jsonl'%lang)
        print(training_output)
        with bz2.open(wikipedia_input, mode="rb") as file, \
                open(training_output,'w',encoding="utf8") as entity_file:
            article_count = 0
            article_text = ""
            article_title = None
            article_id = None
            reading_text = False
            reading_revision = False
            num=0
            for line in file:
                clean_line = line.strip().decode("utf-8")

                if clean_line == "<revision>":
                    reading_revision = True
                elif clean_line == "</revision>":
                    reading_revision = False

                # Start reading new page
                if clean_line == "<page>":
                    article_text = ""
                    article_title = None
                    article_id = None
                # finished reading this page
                elif clean_line == "</page>":
                    if article_id:
                        clean_text, entities = _process_wp_text(
                            article_title, article_text, wp_to_id, lang
                        )
                        if clean_text is not None and entities is not None:
                            _write_training_entities(
                                entity_file, article_id, article_title, clean_text, entities
                            )
                            num+=1
                            # if num==10:
                            #     break
                            article_count += 1
                            if article_count % 10000 == 0 and article_count > 0:
                                logger.info(
                                    "Processed {} articles".format(article_count)
                                )
                            if limit and article_count >= limit:
                                break
                    article_text = ""
                    article_title = None
                    article_id = None
                    reading_text = False
                    reading_revision = False

                # start reading text within a page
                if "<text" in clean_line:
                    reading_text = True

                if reading_text:
                    article_text += " " + clean_line

                # stop reading text within a page (we assume a new page doesn't start on the same line)
                if "</text" in clean_line:
                    reading_text = False

                # read the ID of this article (outside the revision portion of the document)
                if not reading_revision:
                    ids = id_regex.search(clean_line)
                    if ids:
                        article_id = ids[0]
                        # print(article_id)
                        # if article_id in read_ids:
                        #     logger.info(
                        #         "Found duplicate article ID {} {}".format(article_id, clean_line)
                        #     )  # This should never happen ...
                        # read_ids.add(article_id)

                # read the title of this article (outside the revision portion of the document)
                if not reading_revision:
                    titles = title_regex.search(clean_line)
                    if titles:
                        article_title = titles[0].strip()

    logger.info("Finished. Processed {} articles".format(article_count))


def _process_wp_text(article_title, article_text, wp_to_id,lang):
    # ignore meta Wikipedia pages

    if ns_regex.match(article_title):
        return None, None

    # remove the text tags
    text_search = text_tag_regex.sub("", article_text)
    text_search = text_regex.search(text_search)
    if text_search is None:
        return None, None
    text = text_search.group(0)

    # stop processing if this is a redirect page
    if text.startswith("#REDIRECT"):
        return None, None

    # get the raw text without markup etc, keeping only interwiki links
    clean_text = clean(_get_clean_wp_text(text))
    if len(clean_text)==0:
        return None,None

    clean_text, entities = _remove_links(clean_text, wp_to_id,lang)
    # print(clean_text) text
    # print(entities)  list, (entity,id,start,end)
    # exit()
    return clean_text, entities


def _get_clean_wp_text(article_text):
    clean_text = article_text.strip()

    # remove bolding & italic markup
    # clean_text = clean_text.replace("'''", "")
    # clean_text = clean_text.replace("''", "")

    # remove nested {{info}} statements by removing the inner/smallest ones first and iterating
    # try_again = True
    # previous_length = len(clean_text)
    # while try_again: ###去掉{{}}以及其中的内容
    #     clean_text = info_regex.sub(
    #         "", clean_text
    #     )  # non-greedy match excluding a nested {
    #     if len(clean_text) < previous_length:
    #         try_again = True
    #     else:
    #         try_again = False
    #     previous_length = len(clean_text)


    clean_text = html_regex.sub("", clean_text)
    clean_text = category_regex.sub("", clean_text)
    clean_text = file_regex.sub("", clean_text)
    # remove multiple =
    while "==" in clean_text:
        clean_text = clean_text.replace("==", "=")

    clean_text = clean_text.replace(". =", ".")
    clean_text = clean_text.replace(" = ", ". ")
    clean_text = clean_text.replace("= ", ".")
    clean_text = clean_text.replace(" =", "")

    # remove refs (non-greedy match)
    clean_text = ref_regex.sub("", clean_text)
    clean_text = ref_2_regex.sub("", clean_text)

    # remove additional wikiformatting
    clean_text = re.sub(r"&lt;blockquote&gt;", "", clean_text)
    clean_text = re.sub(r"&lt;/blockquote&gt;", "", clean_text)

    # change special characters back to normal ones
    clean_text = clean_text.replace(r"&lt;", "<")
    clean_text = clean_text.replace(r"&gt;", ">")
    clean_text = clean_text.replace(r"&quot;", '"')
    clean_text = clean_text.replace(r"&amp;nbsp;", " ")
    clean_text = clean_text.replace(r"&amp;", "&")

    # remove multiple spaces
    while "  " in clean_text:
        clean_text = clean_text.replace("  ", " ")

    return clean_text.strip()


def clean(text):

    text = dropNested(text,r'{{', r'}}')
    text = dropNested(text, r'{\|', r'\|}')
    text = replaceExternalLinks(text)
    text = magicWordsRE.sub('', text)
    res = ''
    cur = 0
    for m in syntaxhighlight.finditer(text):
        res += unescape(text[cur:m.start()]) + m.group(1)
        cur = m.end()
    text = res + unescape(text[cur:])

    text = bold_italic.sub(r'\1', text)
    text = bold.sub(r'\1', text)
    text = italic_quote.sub(r'\1', text)
    text = italic.sub(r'\1', text)
    text = quote_quote.sub(r'\1', text)
    text = text.replace("'''", '').replace("''", '"')

    spans = []
    # Drop HTML comments
    for m in comment.finditer(text):
        spans.append((m.start(), m.end()))

    # Drop self-closing tags
    for pattern in selfClosing_tag_patterns:
        for m in pattern.finditer(text):
            spans.append((m.start(), m.end()))

    # Drop ignored tags
    for left, right in ignored_tag_patterns:
        for m in left.finditer(text):
            spans.append((m.start(), m.end()))
        for m in right.finditer(text):
            spans.append((m.start(), m.end()))

    # Bulk remove all spans
    text = dropSpans(spans, text)

    # Drop discarded elements
    for tag in discardElements:
        text = dropNested(text, r'<\s*%s\b[^>/]*>' % tag, r'<\s*/\s*%s>' % tag)

    # ori = text
    text = unescape(text)
    # if ori != text:
    #     index = 0
    #     for i,j in enumerate(zip(ori,text)):
    #         if i!=j:
    #             print(ori[index:index+100])
    #             print(text[index:index+100])
    #             exit()
    #         index+=1

    # Expand placeholders
    for pattern, placeholder in placeholder_tag_patterns:
        index = 1
        for match in pattern.finditer(text):
            text = text.replace(match.group(), '%s_%d' % (placeholder, index))
            index += 1

    text = text.replace('<<', u'«').replace('>>', u'»')

    # Cleanup text
    text = text.replace('\t', ' ')
    text = spaces.sub(' ', text)
    text = dots.sub('...', text)
    text = re.sub(u' (,:\.\)\]»)', r'\1', text)
    text = re.sub(u'(\[\(«) ', r'\1', text)
    text = re.sub(r'\n\W+?\n', '\n', text, flags=re.U)  # lines with only punctuations
    text = text.replace(',,', ',').replace(',.', '.')
    return text


def _remove_links(clean_text, wp_to_id,lang):
    # read the text char by char to get the right offsets for the interwiki links
    # with open('text.txt','w') as f:
    #     f.write(clean_text)
    #     exit()
    entities = []
    final_texts = []
    open_read = 0
    reading_text = True
    reading_entity = False
    reading_mention = False
    reading_special_case = False
    entity_buffer = ""
    mention_buffer = ""

    ###分词
    try:
        sentences = Text(clean_text).sentences
    except:
        return None,None

    for i, sentence in enumerate(sentences):
        entity = []
        final_text = ""
        for index, letter in enumerate(sentence):
            if letter == "[":
                open_read += 1
            elif letter == "]":
                open_read -= 1
            elif letter == "|":
                if reading_text:
                    final_text += letter
                # switch from reading entity to mention in the [[entity|mention]] pattern
                elif reading_entity:
                    reading_text = False
                    reading_entity = False
                    reading_mention = True
                else:
                    reading_special_case = True
            else:
                if reading_entity:
                    entity_buffer += letter
                elif reading_mention:
                    mention_buffer += letter
                elif reading_text:
                    final_text += letter
                else:
                    raise ValueError("Not sure at point", clean_text[index - 2 : index + 2])

            if open_read > 2:
                reading_special_case = True

            if open_read == 2 and reading_text:
                reading_text = False
                reading_entity = True
                reading_mention = False

            # we just finished reading an entity
            if open_read == 0 and not reading_text:
                if "#" in entity_buffer or entity_buffer.startswith(":"):
                    reading_special_case = True
                # Ignore cases with nested structures like File: handles etc
                if not reading_special_case:
                    if not mention_buffer:
                        mention_buffer = entity_buffer
                    start = len(final_text)
                    end = start + len(mention_buffer)
                    qid = wp_to_id.get(lang+'_'+entity_buffer, None)
                    if qid:
                        entity.append((mention_buffer, qid, start, end))
                    final_text += mention_buffer

                entity_buffer = ""
                mention_buffer = ""

                reading_text = True
                reading_entity = False
                reading_mention = False
                reading_special_case = False

        ###一个句子读完，final_text有了，entities是这个句子含有的entities
        tmp_i = i
        # print(final_text)
        if len(final_text)==0:
            continue
        # while len(Text(final_text).words) < 64 and tmp_i < len(sentences) - 1:
        #     tmp_i +=1
        #     final_text+=sentences[tmp_i].raw
            # print(Text(final_text).words)
        # try:
        #     while len(Text(final_text).words)<64 and tmp_i<len(sentences)-1:
        #         tmp_i +=1
        #         final_text+=sentences[tmp_i].raw
        # except:
        #     print(final_text)
        #     exit()

        final_texts.append(final_text)
        entities.append(entity)

    return final_texts, entities


def _write_training_description(outputfile, qid, description):
    if description is not None:
        line = str(qid) + "|" + description + "\n"
        outputfile.write(line)


def _write_training_entities(outputfile, article_id, article_title, clean_text, entities):

    for i in range(len(clean_text)):
        text = clean_text[i]
        entity_list = entities[i]
        if len(entity_list)>0:
            ####text变长
            tmp_i = i
            while len(Text(text).words)<64 and tmp_i<len(clean_text)-1:
                tmp_i+=1
                text+=clean_text[tmp_i]

            for ent in entity_list:
                line = (
                        json.dumps(
                            {
                                "article_id": article_id,
                                'article_title':article_title,
                                "context": text,
                                "entity": ent[1],
                                'mention':ent[0],
                                'start':ent[2],
                                'end':ent[3]
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                )
                outputfile.write(line)


def read_training_indices(entity_file_path):
    """ This method creates two lists of indices into the training file: one with indices for the
     training examples, and one for the dev examples."""
    train_indices = []
    dev_indices = []

    with entity_file_path.open("r", encoding="utf8") as file:
        for i, line in enumerate(file):
            example = json.loads(line)
            article_id = example["article_id"]
            clean_text = example["clean_text"]

            if is_valid_article(clean_text):
                if is_dev(article_id):
                    dev_indices.append(i)
                else:
                    train_indices.append(i)

    return train_indices, dev_indices




def is_dev(article_id):
    if not article_id:
        return False
    return article_id.endswith("3")


def is_valid_article(doc_text):
    # custom length cut-off
    return 10 < len(doc_text) < 30000


def is_valid_sentence(sent_text):
    if not 10 < len(sent_text) < 3000:
        # custom length cut-off
        return False

    if sent_text.strip().startswith("*") or sent_text.strip().startswith("#"):
        # remove 'enumeration' sentences (occurs often on Wikipedia)
        return False

    return True

if __name__=='__main__':
    text = "e arranged &lt ==ffjkrjk== {{for}} an [[in]] <!--ejfklwejklfjwole--> termediary to https://www.perseus.tufts.edu/hopper/text?doc=Plat.+Sym.+180a 180a; inquire into Grant's political i"
    clean(text)