package stream

import (
	"time"

	"cloud.google.com/go/spanner"
)

type ChangeRecord struct {
	DataChangeRecords      DataChangeRecords        `spanner:"data_change_record"`
	HeartbeatRecords       []*HeartbeatRecord       `spanner:"heartbeat_record"`
	ChildPartitionsRecords []*ChildPartitionsRecord `spanner:"child_partitions_record"`
}

type DataChangeRecords []*DataChangeRecord

type DataChangeRecord struct {
	CommitTimestamp                      time.Time   `spanner:"commit_timestamp"`
	RecordSequence                       string      `spanner:"record_sequence"`
	ServerTransactionID                  string      `spanner:"server_transaction_id"`
	IsLastRecordInTransactionInPartition bool        `spanner:"is_last_record_in_transaction_in_partition"`
	TableName                            string      `spanner:"table_name"`
	ColumnTypes                          ColumnTypes `spanner:"column_types"`
	Mods                                 []*Mod      `spanner:"mods"`
	ModType                              string      `spanner:"mod_type"`
	ValueCaptureType                     string      `spanner:"value_capture_type"`
	NumberOfRecordsInTransaction         int64       `spanner:"number_of_records_in_transaction"`
	NumberOfPartitionsInTransaction      int64       `spanner:"number_of_partitions_in_transaction"`
	TransactionTag                       string      `spanner:"transaction_tag"`
	IsSystemTransaction                  bool        `spanner:"is_system_transaction"`
}

func (r *DataChangeRecord) PrimaryKeySets() spanner.KeySet {
	primaryKeys := r.ColumnTypes.PrimaryKeys()
	keys := make([]spanner.KeySet, len(r.Mods))
	for i, mod := range r.Mods {
		kvs, ok := mod.Keys.Value.(map[string]interface{})
		if !ok {
			continue
		}
		pks := make(spanner.Key, len(primaryKeys))
		for i, key := range primaryKeys {
			pks[i] = kvs[key]
		}
		keys[i] = pks
	}
	return spanner.KeySets(keys...)
}

type ColumnTypes []*ColumnType

func (ts ColumnTypes) PrimaryKeys() []string {
	pks := make([]string, 0, 1)
	for _, column := range ts {
		if column.IsPrimaryKey {
			pks = append(pks, column.Name)
		}
	}
	return pks
}

type ColumnType struct {
	Name            string           `spanner:"name"`
	Type            spanner.NullJSON `spanner:"type"`
	IsPrimaryKey    bool             `spanner:"is_primary_key"`
	OrdinalPosition int64            `spanner:"ordinal_position"`
}

type Mod struct {
	Keys      spanner.NullJSON `spanner:"keys"`
	NewValues spanner.NullJSON `spanner:"new_values"`
	OldValues spanner.NullJSON `spanner:"old_values"`
}

type HeartbeatRecord struct {
	Timestamp time.Time `spanner:"timestamp"`
}

type ChildPartitionsRecord struct {
	StartTimestamp  time.Time         `spanner:"start_timestamp"`
	RecordSequence  string            `spanner:"record_sequence"`
	ChildPartitions []*ChildPartition `spanner:"child_partitions"`
}

type ChildPartition struct {
	Token                 string   `spanner:"token"`
	ParentPartitionTokens []string `spanner:"parent_partition_tokens"`
}
