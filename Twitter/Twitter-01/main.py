# -*- coding: utf-8 -*-
"""
Created on Fri Dec  8 10:50:50 2017

@author: atholon
"""
import time
import calendar
import string
import operator

import json

import csv

import tweepy

from Storage import Storage
from FrenchStemmer import FrenchStemmer

from creds import get_tweepy_api


# Tweepy documentation
# http://docs.tweepy.org/en/v3.5.0/
# https://media.readthedocs.org/pdf/tweepy/latest/tweepy.pdf


def search_tweet_backward_single_request(api, output_storage, max_id=None):
    """issue a single status search request from a given tweet id going backward.
    results are appended to the output file.
    Return values :
    - number of status found
    - id of the oldest status
    """
    search_params = dict()
    search_params['lang'] = 'fr'
    search_params['count'] = 100
    search_params['geocode'] = '43.834686,6.126635,200km'
    if max_id is not None:
        search_params['max_id'] = max_id  # as string

    search_results = api.search(**search_params)

    print(len(search_results), search_results.max_id, search_results.since_id)
    if len(search_results) > 0:
        print(search_results[0])

    output_data_file, remaining_size = output_storage.get_current_data_file()
    with output_data_file.open('a') as tweet_file:
        for tweet in search_results:
            s = json.dumps(tweet._json)
            print(s, file=tweet_file)

    return len(search_results), search_results.max_id


def search_tweet_backward_all_requests(api, output_storage, max_id=None):
    rate_limit_status = api.rate_limit_status()
    search_limits = rate_limit_status['resources']['search']['/search/tweets']
    # reset time is epoch time
    reset_time = search_limits['reset']
    remaining_searches = search_limits['remaining'] - 5
    next_max_id = ''
    while remaining_searches > 0 and next_max_id is not None:
        nb_results, next_max_id = search_tweet_backward_single_request(api, output_storage, max_id)
        max_id = next_max_id

        now = calendar.timegm(time.gmtime())
        if now < reset_time:
            remaining_time = reset_time - now
            delay = remaining_time / remaining_searches
            time.sleep(delay)

        remaining_searches -= 1

    now = calendar.timegm(time.gmtime())
    if now < reset_time:
        remaining_time = reset_time - now
    else:
        remaining_time = 0

    return max_id, remaining_time


def search_tweet_backward_rate_limited(api, output_storage, duration, max_id=None):
    next_max_id = ''
    stop_timegm = calendar.timegm(time.gmtime()) + duration
    while calendar.timegm(time.gmtime()) < stop_timegm and next_max_id is not None:
        next_max_id, remaining_time = search_tweet_backward_all_requests(api, output_storage, max_id)
        max_id = next_max_id
        time.sleep(remaining_time + 1)


def drop_retweet(api, input_storage, output_storage):
    remaining_size = 0
    output_data_file = None

    for input_data_file in input_storage.get_all_data_files():
        with input_data_file.open() as input_tweet_file:
            for line in input_tweet_file:
                obj = json.loads(line)
                status = tweepy.ModelFactory.status.parse(api, obj)
                #  drop retweet
                if hasattr(status, 'retweeted_status'):
                    pass
                else:
                    if output_data_file is None:
                        output_data_file, remaining_size = output_storage.get_current_data_file()
                        output_file = output_data_file.open(mode='w')

                    remaining_size -= output_file.write(line)

                    if remaining_size < 0:
                        output_file.close()
                        output_data_file = None

    if output_data_file is not None:
        output_file.close()


def get_translate_table():
    # prepare character map for latin-1, https://en.wikipedia.org/wiki/ISO/IEC_8859-1
    source_characters = list(string.ascii_uppercase)
    destination_characters = list(string.ascii_lowercase)

    for character in range(0xC0, 0xE0):
        if character in (0xD0, 0xDE, 0xDF):
            pass
        elif character == 0xD7:
            source_characters.append(chr(character))
            destination_characters.append(' ')
        else:
            source_characters.append(chr(character))
            destination_characters.append(chr(character + 0x20))

    for character_range in (range(0x00, 0x30), range(0x3A, 0x41), range(0x5B, 0x61), range(0x7B, 0xC0)):
        for special_character in character_range:
            source_characters.append(chr(special_character))
            destination_characters.append(' ')

    source_characters.append(chr(0xF7))
    destination_characters.append(' ')

    source_characters = ''.join(source_characters)
    destination_characters = ''.join(destination_characters)
    translate_table = str.maketrans(source_characters, destination_characters)

    return translate_table


def get_core_text(status):
    status_text = list(status.text)
    for entity, values in status.entities.items():
        if entity in ('hashtags', 'symbols', 'polls'):
            # nothing to do for those entities
            pass
        elif entity in ('media', 'urls', 'user_mentions'):
            for value in values:
                status_text[value['indices'][0]:value['indices'][1]] = ' ' * (value['indices'][1] - value['indices'][0])
        else:
            print("warning: unexpected entity {} = {}".format(entity, values))

    # back to string removing extra spaces
    core_text = ''.join(status_text)
    core_text = ' '.join(core_text.split())

    return core_text


def extract_core_text(api, input_storage, output_storage):
    remaining_size = 0
    output_data_file = None

    core_status = dict()
    for input_data_file in input_storage.get_all_data_files():
        with input_data_file.open() as input_tweet_file:
            for line in input_tweet_file:
                obj = json.loads(line)
                status = tweepy.ModelFactory.status.parse(api, obj)

                # get the text written by the user (excluding user mentions and urls)
                core_text = get_core_text(status)

                core_status['id_str'] = status.id_str
                core_status['core_text'] = core_text

                if output_data_file is None:
                    output_data_file, remaining_size = output_storage.get_current_data_file()
                    output_file = output_data_file.open(mode='w')

                remaining_size -= output_file.write(json.dumps(core_status))
                remaining_size -= output_file.write('\n')

                if remaining_size < 0:
                    output_file.close()
                    output_data_file = None

    if output_data_file is not None:
        output_file.close()



def extract_user(api, input_storage, output_storage):
    remaining_size = 0
    output_data_file = None

    for input_data_file in input_storage.get_all_data_files():
        with input_data_file.open() as input_tweet_file:
            for line in input_tweet_file:
                obj = json.loads(line)
                status = tweepy.ModelFactory.status.parse(api, obj)

                user = status.user
                if output_data_file is None:
                    output_data_file, remaining_size = output_storage.get_current_data_file()
                    output_file = output_data_file.open(mode='w')

                remaining_size -= output_file.write(json.dumps(user._json))
                remaining_size -= output_file.write('\n')

                if remaining_size < 0:
                    output_file.close()
                    output_data_file = None

    if output_data_file is not None:
        output_file.close()


def extract_user_urls(api, input_storage, output_storage):
    remaining_size = 0
    output_data_file = None


    user = dict()
    for input_data_file in input_storage.get_all_data_files():
        with input_data_file.open() as input_tweet_file:
            for line in input_tweet_file:
                obj = json.loads(line)

                user.clear()
                user['id_str'] = obj['id_str']
                user['name'] = obj['name']
                user['screen_name'] = obj['screen_name']

                if obj['url'] is not None:
                    user['expanded_url'] = obj['entities']['url']['urls'][0]['expanded_url']
                else:
                    user['expanded_url'] = ''

                if obj['default_profile_image']:
                    user['profile_image_url'] = ''
                else:
                    user['profile_image_url'] = obj['profile_image_url']

                if 'profile_banner_url' in obj and obj['profile_banner_url'] is not None:
                    user['profile_banner_url'] = obj['profile_banner_url']
                else:
                    user['profile_image_url'] = ''


                if output_data_file is None:
                    output_data_file, remaining_size = output_storage.get_current_data_file()
                    output_file = output_data_file.open(mode='w')

                remaining_size -= output_file.write(json.dumps(user))
                remaining_size -= output_file.write('\n')

                if remaining_size < 0:
                    output_file.close()
                    output_data_file = None

    if output_data_file is not None:
        output_file.close()


def get_vocabulary(input_storage):
    vocabulary = dict()

    # translate table to normalize text (only keeps numbers and characters; lowering the later)
    translate_table = get_translate_table()

    # french stemmer
    # french_stemmer = FrenchStemmer()

    for input_data_file in input_storage.get_all_data_files():
        with input_data_file.open() as input_tweet_file:
            for line in input_tweet_file:
                core_status = json.loads(line)

                core_text = core_status['core_text']

                # get rid of all non latin-1 characters
                core_text = core_text.encode("latin_1", "replace").decode("latin_1")

                # normalize text
                core_text = core_text.translate(translate_table)

                words = core_text.split()
                # words = french_stemmer.get_stems(core_text)

                for word in words:
                    if word in vocabulary:
                        vocabulary[word] += 1
                    else:
                        vocabulary[word] = 1

    vocabulary_sorted = sorted(vocabulary.items(), key=operator.itemgetter(1), reverse=True)

    return vocabulary_sorted


def export_to_cvs(input_storage):
    csv_encoding = "latin_1"
    with open('tweets.csv', 'w', newline='', encoding=csv_encoding) as csv_file:
        csv_writer = csv.writer(csv_file)

        for input_data_file in input_storage.get_all_data_files():
            with input_data_file.open() as input_tweet_file:
                for line in input_tweet_file:
                    core_status = json.loads(line)
                    tweet_text = core_status['core_text'].encode(csv_encoding, "replace").decode(csv_encoding)

                    csv_writer.writerow([core_status['id_str'], tweet_text])


def main():
    api = get_tweepy_api()
    if False:
        input_storage = Storage('dataset-01-01', 'tweet-01-01')
        output_storage = Storage('dataset-01-02', 'tweet-01-02')
        # process the entire data set, make sure output is empty first
        if output_storage.is_empty():
            drop_retweet(api, input_storage, output_storage)
        else:
            print("ERROR output data set not empty {}".format(output_storage.data_set_dir))

    if False:
        input_storage = Storage('dataset-01-01', 'tweet-01-01')
        output_storage = Storage('user-01-01', 'user-01-01')
        # process the entire data set, make sure output is empty first
        if output_storage.is_empty():
            extract_user(api, input_storage, output_storage)
        else:
            print("ERROR output data set not empty {}".format(output_storage.data_set_dir))

    if True:
        input_storage = Storage('user-01-01', 'user-01-01')
        output_storage = Storage('user-01-02', 'user-01-02')
        # process the entire data set, make sure output is empty first
        if output_storage.is_empty():
            extract_user_urls(api, input_storage, output_storage)
        else:
            print("ERROR output data set not empty {}".format(output_storage.data_set_dir))

    if False:
        input_storage = Storage('dataset-01-02', 'tweet-01-02')
        output_storage = Storage('dataset-01-03', 'tweet-01-03')
        # process the entire data set, make sure output is empty first
        if output_storage.is_empty():
            extract_core_text(api, input_storage, output_storage)
        else:
            print("ERROR output data set not empty {}".format(output_storage.data_set_dir))

    if False:
        input_storage = Storage('dataset-01-03', 'tweet-01-03')

        vocabulary_sorted = get_vocabulary(input_storage)
        with open('vocabulary.txt', 'w', encoding="latin_1") as output_file:
            for word, nb in vocabulary_sorted:
                print('{},{}'.format(word, nb), file=output_file)

    if False:
        input_storage = Storage('dataset-01-03', 'tweet-01-03')
        export_to_cvs(input_storage)

    if False:
        output_storage = Storage('test-set', 'test')
        duration = 60
        max_id = 944653855807156223
        # output_storage = Storage('dataset-01-01', 'tweet-01-01')
        # duration = 9 * 4 * 900
        search_tweet_backward_rate_limited(api, output_storage, duration=duration, max_id=max_id)

    if False:
        pass
        # post retrieval ingest:
        storage = Storage('dataset-01-01', 'tweet-01-01')
        storage.add_file_to_storage('C:\\DSTI\\CA Project Technical\\data\\twitter\\dataset-01-01\\tweet-big-010.txt')


if __name__ == '__main__':
    main()
