# -*- encoding : utf-8 -*-
require_relative '../spec_helper'

describe Cequel::Metal::PreparedStatementManager do
  before :all do
    cequel.schema.create_table(:posts) do
      key :id, :int
      column :title, :text
      column :body, :text
    end
  end

  after :each do
    ids = cequel[:posts].select(:id).map { |row| row[:id] }
    cequel[:posts].where(id: ids).delete if ids.any?
  end

  after :all do
    cequel.schema.drop_table(:posts)
  end

  subject { Cequel::Metal::PreparedStatementManager.new cequel }

  describe '#prepared' do
    before (:each) { subject.prepared('SELECT * from posts WHERE id = ?') }

    it 'should prepare a new statement' do
      expect(subject.size).to eq 1
    end
    it 'should not prepare the same statement twice' do
      expect(subject.size).to eq 1
      subject.prepared('SELECT * from posts WHERE id = ?')
      expect(subject.size).to eq 1
    end
  end

  describe '#reset' do
    before (:each) { subject.prepared('SELECT * from posts WHERE id = ?') }

    it 'should reset to have zero prepared statements' do
      subject.reset
      expect(subject.size).to eq 0
    end
  end

end
