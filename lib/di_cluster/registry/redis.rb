require 'redis'
require 'redis-namespace'
require 'msgpack'

module DiCluster
  module Registry
    class Redis
      def initialize(options = {})
        options = options.each_with_object({}) { |(k,v), obj| obj[k.to_sym] = v }
        environment = DiCluster.environment
        @redis = ::Redis::Namespace.new "di_cluster_#{environment}", redis: ::Redis.new(options)
      end

      def nodes
        nodes_cleanup
        keys('nodes')
      end

      def nodes_with_role(role_name)
        nodes_cleanup
        Hash[all('nodes').map { |k, v| [k, MessagePack.unpack(v)] }].select { |k,v| v["roles"].include?(role_name) }
      end

      def nodes_cleanup
        nodes = keys('nodes')
        nodes.each { |node| unregister(node) if node(node)['ttl'] < Time.now.to_i }
      end

      def node(node_name)
        get('nodes', node_name)
      end
      alias :[] :node

      def register(node_name, options)
        set('nodes', node_name, options)
      end

      def update(node_name, options)
        value = get('nodes', node_name)
        set('nodes', node_name, value.merge(options))
      end

      def unregister(node_name)
        remove('nodes', node_name)
      end

      def clear_nodes
        delete('nodes')
      end

      private

      def all(table)
        @redis.hgetall(table)
      end

      def keys(table)
        @redis.hkeys(table)
      end

      def set(table, key, value)
        @redis.hset table, key.to_s, value.to_msgpack
      end

      def get(table, key)
        value = @redis.hget table, key.to_s
        value = MessagePack.unpack(value) if value
        value
      end

      def remove(table, key)
        @redis.hdel table, key
      end

      def delete(table)
        @redis.del table
      end
    end
  end
end
