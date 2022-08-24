import pickle
from typing import List, Optional

from collect.types.mics import Mic

from collect.listing_store.listing_store import ListingStore, ListingId, Listing, Attributes


class PickleStore(ListingStore):
    listings: dict[ListingId, Listing]

    def __init__(self, filename):
        f = open(filename, "rb")
        self.listings = {l.id: l for l in pickle.load(f)}

    def all(self) -> List[Listing]:
        return [l for l in self.listings.values()]

    def list(self, mic: Mic) -> List[Listing]:
        return [l for l in self.all() if l.id.mic == mic]

    def lookup(self, listing_id: ListingId) -> Optional[Listing]:
        return self.listings.get(listing_id, None)

    def lookup_by_symbol(self, mic: Mic, symbol: str) -> List[Listing]:
        return [l for l in self.list(mic) if l.exchange_symbol == symbol]

    def get_attributes(self, listing_id: ListingId) -> Optional[Attributes]:
        if l := self.lookup(listing_id) is not None:
            return l.attributes
        return None
