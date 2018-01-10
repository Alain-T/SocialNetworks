import string


class FrenchStemmer:
    """French Stemmer class"""

    # class variable
    _language_stems = None

    def __init__(self):

        def read_stem_file():
            stem_file_path = 'OLDlexique.txt'

            source_characters = list(string.ascii_uppercase)
            destination_characters = list(string.ascii_lowercase)

            for character_range in (range(0x00, 0x30), range(0x3A, 0x41), range(0x5B, 0x61), range(0x7B, 0xC0)):
                for special_character in character_range:
                    source_characters.append(chr(special_character))
                    destination_characters.append(' ')

            source_characters = ''.join(source_characters)
            destination_characters = ''.join(destination_characters)
            trantab = str.maketrans(source_characters, destination_characters)

            language_stems = dict()
            with open(stem_file_path, 'r') as stem_file:
                for line in stem_file:
                    words = line.split()
                    if words[1] == '=':
                        language_stems[words[0]] = words[0].translate(trantab).split()
                    else:
                        language_stems[words[0]] = words[1].translate(trantab).split()
            return language_stems

        # check class variable
        if type(self)._language_stems is None:
            type(self)._language_stems = read_stem_file()

        # assign class variable to instance for ease of use
        self._language_stems = type(self)._language_stems

    def get_stems(self, s):
        stems = list()
        for word in s.split():
            if word in self._language_stems:
                stems.extend(self._language_stems[word])
            else:
                stems.append(word)

        return stems


def test_french_stemmer():
    stemmer = FrenchStemmer()
    print(stemmer.get_stems(''))
    print(stemmer.get_stems('a'))
    print(stemmer.get_stems('aimerai azerty'))

    # break the language stemmer dictionnary, to check its unicity
    del FrenchStemmer._language_stems['aimerai']
    stemmer = FrenchStemmer()
    print(stemmer.get_stems('aimerai azerty'))


if __name__ == '__main__':
    test_french_stemmer()
