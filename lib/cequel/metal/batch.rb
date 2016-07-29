# -*- encoding : utf-8 -*-
require 'stringio'

module Cequel
  module Metal
    #
    # Encapsulates a batch operation
    #
    # @see Keyspace::batch
    # @api private
    #
    class Batch
      #
      # @param keyspace [Keyspace] the keyspace that this batch will be
      #   executed on
      # @param options [Hash]
      # @option options [Integer] :auto_apply If specified, flush the batch
      #   after this many statements have been added.
      # @option options [Boolean] :unlogged (false) Whether to use an [unlogged
      #   batch](
      #   http://www.datastax.com/dev/blog/atomic-batches-in-cassandra-1-2).
      #   Logged batches guarantee atomicity (but not isolation) at the
      #   cost of a performance penalty; unlogged batches are useful for bulk
      #   write operations but behave the same as discrete writes.
      # @see Keyspace#batch
      #
      def initialize(keyspace, options = {})
        options.assert_valid_keys(:auto_apply, :unlogged, :consistency, :prepared)
        @keyspace = keyspace
        @auto_apply = options[:auto_apply]
        @unlogged = options.fetch(:unlogged, false)
        @prepared = options.fetch(:prepared, false)
        @consistency = options.fetch(:consistency,
                                     keyspace.default_consistency)
        reset
      end

      #
      # Add a statement to the batch.
      #
      # @param (see Keyspace#execute)
      #
      def execute(cql, bind_vars)
        @statements << Statement.new(cql, bind_vars)
        if @auto_apply && @statements.size >= @auto_apply
          apply
          reset
        end
      end

      #
      # Send the batch to Cassandra
      #
      def apply
        return if @statements.size.zero?
        options = { consistency: @consistency, prepared: @prepared }
        if @statements.size > 1
          @keyspace.execute_batch_with_options(@statements, options.merge(logged: logged?))
        else
          statement = @statements.first
          @keyspace.execute_with_options(statement.cql, statement.bind_vars, options)
        end

        execute_on_complete_hooks
      end

      def on_complete(&block)
        on_complete_hooks << block
      end

      #
      # Is this an unlogged batch?
      #
      # @return [Boolean]
      def unlogged?
        @unlogged
      end

      #
      # Is this a logged batch?
      #
      # @return [Boolean]
      #
      def logged?
        !unlogged?
      end

      # @private
      def execute_with_consistency(cql, bind_vars, query_consistency)
        if query_consistency && query_consistency != @consistency
          raise ArgumentError,
                "Attempting to perform query with consistency " \
                "#{query_consistency.to_s.upcase} in batch with consistency " \
                "#{@consistency.upcase}"
        end
        execute(cql, bind_vars)
      end

      # @private
      def execute_with_options(cql, bind_vars, options)
        query_consistency = options.fetch(:consistency, nil)
        if query_consistency && query_consistency != @consistency
          raise ArgumentError,
            "Attempting to perform query with consistency " \
                "#{query_consistency.to_s.upcase} in batch with consistency " \
                "#{@consistency.upcase}"
        end
        execute(cql, bind_vars)
      end

      private

      attr_reader :on_complete_hooks

      def reset
        @statements = []
        @on_complete_hooks = []
      end

      def execute_on_complete_hooks
        on_complete_hooks.each { |hook| hook.call }
      end
    end
  end
end
