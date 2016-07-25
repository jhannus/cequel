# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Encapsulates an `INSERT` statement
    #
    # @see DataSet#insert
    # @since 1.0.0
    #
    class Inserter < Writer
      #
      # (see Writer#initialize)
      #
      def initialize(data_set)
        @row = {}
        super
      end

      #
      # (see Writer#execute)
      #
      def execute(options = {})
        statement = Statement.new
        ttl = options.fetch(:ttl, nil)
        timestamp = options.fetch(:timestamp, nil)
        options = options.merge(consistency: data_set.query_consistency, prepared_statement: data_set.prepared_statement)
        write_to_statement(statement, options)

        bind_vars = statement.bind_vars
        if ttl
          bind_vars = bind_vars.unshift(ttl)
        end
        if timestamp
          bind_vars = bind_vars.unshift((timestamp.to_f * 1_000_000).to_i)
        end

        data_set.write_with_consistency(
          statement.cql, bind_vars, data_set.query_consistency)
      end

      #
      # Insert the given data into the table
      #
      # @param data [Hash<Symbol,Object>] map of column names to values
      # @return [void]
      #
      def insert(data)
        @row.merge!(data.symbolize_keys)
      end

      private

      attr_reader :row

      def column_names
        row.keys
      end

      def statements
        [].tap do |statements|
          row.each_pair do |column_name, value|
            column_names << column_name
            prepare_upsert_value(value) do |statement, *values|
              statements << statement
              bind_vars.concat(values)
              #puts "prepare_upsert_value: yeild: #{values} #{statements} #{column_names}"
            end
          end
        end
      end

      def write_to_statement(statement, options)
        statement.append("INSERT INTO #{table_name}")
        statement.append(
          " (#{column_names.join(', ')}) VALUES (#{statements.join(', ')}) ",
          *bind_vars)
        statement.append(generate_upsert_options(options))
      end
    end
  end
end
