import pickle

class PickleStore:

    def __init__(self, filename):
        f = open(filename, "rb")
        self.listings = {l.id: l for l in pickle.load(f)}

    def all(self):
        return [l for l in self.listings.values()]

    def list(self, mic):
        return [l for l in self.all() if l.id.mic == mic]

    def lookup(self, listing_id):
        return self.listings.get(listing_id, None)

    def lookup_by_symbol(self, mic, symbol: str):
        return [l for l in self.list(mic) if l.exchange_symbol == symbol]

    def get_attributes(self, listing_id):
        if l := self.lookup(listing_id) is not None:
            return l.attributes
        return None
