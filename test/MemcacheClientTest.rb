require 'rubygems'
require 'memcache'
require 'test/unit'
class ErlyCacheTest < Test::Unit::TestCase
   def setup
     @cache = MemCache::new 'localhost:11212',
                       :debug => true,
                       :namespace => 'test'
   end
   def test_basic
    assert @cache.active?
    @cache.set("a",100)
    assert_equal 100,@cache["a"]
    assert_equal 100,@cache.get("a")
   end
   def tear_down
   end
end

 
