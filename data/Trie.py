# -*- coding: utf-8 -*-

class TrieNode(object):

    def __init__(self, alphabetSize):
        self.children = [None] * alphabetSize

class TrieLeaf(object):

    def __init__(self, idx):
        self.indexs = [idx]

class Trie(object):

    def __init__(self, wordSize, alphabetSize):
        self.root = TrieNode(alphabetSize)
        self.wordSize = wordSize
        self.alphabetSize = alphabetSize

    def add(self, s, data_idx):
        """Add a string to this trie."""
        p = self.root
        for i in range(self.wordSize - 1):
            idx = ord(s[i]) - ord('a')
            if p.children[idx] is None:
                new_node = TrieNode(self.alphabetSize)
                p.children[idx] = new_node
                p = new_node
            else:
                p = p.children[idx]
        last = ord(s[self.wordSize - 1]) - ord('a')
        if p.children[last] is None:
            new_leaf = TrieLeaf(data_idx)
            p.children[last] = new_leaf
        else:
            p.children[last].indexs.append(data_idx)

    def similar(self, s, n, pred):
        similar_list =  []
        p = self.root
        for i in range(self.wordSize):
            idx = ord(s[i]) - ord('a')
            p = p.children[idx]
        similar_list += p.indexs
        similar_list = filter(pred, similar_list)
        if len(similar_list) >= n:
            return similar_list
        for i in range(self.wordSize):
            p = self.root
            for j in range(i):
                idx = ord(s[j]) - ord('a')
                p =p.children[idx]
            for k in range(self.alphabetSize):
                if k == ord(s[i]) - ord('a'):
                    continue
                pp = p.children[k]
                if pp is None:
                    continue
                for l in range(self.wordSize - i - 1):
                    idx = ord(s[i + l + 1]) - ord('a')
                    pp = pp.children[idx]
                similar_list += pp.indexs
            similar_list = filter(pred, similar_list)    
            if len(similar_list) >= n:
                return similar_list
        return similar_list



if __name__ == '__main__':
    trie = Trie(wordSize=5, alphabetSize=4)
    trie.add('aabbc', 1)
    trie.add('adbbc', 2)
    trie.add('dabbc', 3)
    trie.add('aadbc', 4)
    trie.add('aabdc', 5)
    trie.add('aabbd', 6)
    trie.add('aabbc', 7)
    trie.add('acbbc', 8)
    trie.add('cabbc', 9)
    trie.add('aadcc', 10)
    trie.add('aabac', 11)
    trie.add('aabbb', 12)
    window = 4
    def predicate(i):
        def my_pred(t):
            return True if abs(i - t) >= window else False
        return my_pred

    s = trie.similar('aabbc', 4, predicate(1))
    print s
