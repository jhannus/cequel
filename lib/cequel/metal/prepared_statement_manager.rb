# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Manages prepared statements.
    #
    # @api private
    #
    class PreparedStatementManager
      #
      # @param keyspace [Keyspace] keyspace to prepare statements for
      # @api private
      #
      def initialize(keyspace)
        @keyspace = keyspace
        reset
      end

      def prepared(cql)
        unless @statements[cql]
          @statements[cql] = @keyspace.client.prepare(cql)
        end
        @statements[cql]
      end

      def [](cql)
        @statements[cql]
      end

      def reset
        @statements = {}
      end

      def size
        @statements.size
      end
    end
  end
end
