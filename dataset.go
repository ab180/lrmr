package lrmr

import (
	"fmt"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/transformation"
	"reflect"
)

// Dataset is less-resilient distributed dataset
type Dataset struct {
	Session
	NumStages int
}

func Input(provider InputProvider, m *Master) *Dataset {
	sess := NewSession(m)
	sess.AddStage("__input", &inputProviderWrapper{provider: provider})
	return &Dataset{Session: sess}
}

func TextFile(uri string, m *Master) *Dataset {
	sess := NewSession(m)
	sess.AddStage("__input", &localInput{Path: uri})
	return &Dataset{Session: sess}
}

func (d *Dataset) Map(m transformation.Mapper) *Dataset {
	d.Session.AddStage(d.stageName(m), m)
	return d
}

func (d *Dataset) Then(m transformation.Transformation) *Dataset {
	d.Session.AddStage(d.stageName(m), m)
	return d
}

func (d *Dataset) GroupByKey(keyColumn string) *Dataset {
	d.Session.Output(node.DescribingStageOutput().
		WithPartitions(keyColumn))
	return d
}

func (d *Dataset) GroupByKnownKeys(column string, knownKeys []string) *Dataset {
	d.Session.Output(node.DescribingStageOutput().
		WithFixedPartitions(column, knownKeys))
	return d
}

func (d *Dataset) NoOutput() *Dataset {
	d.Session.Output(node.DescribingStageOutput().Nothing())
	return d
}

func (d *Dataset) stageName(i interface{}) string {
	typ := reflect.TypeOf(i)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	name := fmt.Sprintf("%s%d", typ.Name(), d.NumStages)
	d.NumStages += 1
	return name
}
