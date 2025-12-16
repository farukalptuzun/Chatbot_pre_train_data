"""
Deduplication module
Exact deduplication using hashing and fuzzy deduplication using MinHash
"""
import hashlib
from typing import Set, Optional
from config import config


class Deduplicator:
    """Deduplication handler with exact and fuzzy dedup"""
    
    def __init__(self):
        self.exact_seen: Set[str] = set()
        self.lsh = None
        
        if config.fuzzy_dedup_enabled:
            try:
                from datasketch import MinHashLSH
                self.lsh = MinHashLSH(
                    threshold=config.fuzzy_similarity_threshold,
                    num_perm=config.minhash_num_perm
                )
            except ImportError:
                print("Warning: datasketch not installed. Fuzzy dedup disabled.")
                print("Install with: pip install datasketch")
                config.fuzzy_dedup_enabled = False
    
    def _get_hash(self, text: str) -> str:
        """Get MD5 hash of text for exact dedup"""
        return hashlib.md5(text.encode("utf-8")).hexdigest()
    
    def _get_minhash(self, text: str):
        """Get MinHash of text for fuzzy dedup"""
        from datasketch import MinHash
        
        m = MinHash(num_perm=config.minhash_num_perm)
        # Use word-based hashing (split by whitespace)
        words = set(text.split())
        for word in words:
            m.update(word.encode("utf-8"))
        return m
    
    def exact_dedup(self, text: str) -> bool:
        """
        Check if exact duplicate exists
        
        Args:
            text: Input text
            
        Returns:
            True if text is NOT a duplicate, False if duplicate found
        """
        if not config.exact_dedup_enabled:
            return True
        
        h = self._get_hash(text)
        if h in self.exact_seen:
            return False
        
        self.exact_seen.add(h)
        return True
    
    def fuzzy_dedup(self, text: str) -> bool:
        """
        Check if fuzzy duplicate exists using MinHash LSH
        
        Args:
            text: Input text
            
        Returns:
            True if text is NOT a duplicate, False if duplicate found
        """
        if not config.fuzzy_dedup_enabled or self.lsh is None:
            return True
        
        try:
            m = self._get_minhash(text)
            
            # Check for similar texts
            results = self.lsh.query(m)
            if len(results) > 0:
                return False  # Similar text found
            
            # Add this text's hash to LSH
            text_hash = self._get_hash(text)
            self.lsh.insert(text_hash, m)
            
            return True
        except Exception as e:
            print(f"Fuzzy dedup error: {e}")
            return True  # On error, allow the text
    
    def is_duplicate(self, text: str) -> bool:
        """
        Check both exact and fuzzy duplicates
        
        Args:
            text: Input text
            
        Returns:
            True if text is NOT a duplicate, False if duplicate found
        """
        # First check exact duplicates (faster)
        if not self.exact_dedup(text):
            return True  # Is duplicate
        
        # Then check fuzzy duplicates
        if not self.fuzzy_dedup(text):
            return True  # Is duplicate
        
        return False  # Not a duplicate


# Global deduplicator instance
_deduplicator: Optional[Deduplicator] = None


def get_deduplicator() -> Deduplicator:
    """Get or create global deduplicator instance"""
    global _deduplicator
    if _deduplicator is None:
        _deduplicator = Deduplicator()
    return _deduplicator


def reset_deduplicator():
    """Reset deduplicator (useful for new dataset processing)"""
    global _deduplicator
    _deduplicator = None

