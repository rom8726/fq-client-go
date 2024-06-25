package fq

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseMultiResponse(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    multiResponseStruct
		wantErr bool
	}{
		{
			name: "1 part",
			args: args{
				data: []byte("ok|123"),
			},
			want: multiResponseStruct{
				status: ResponseStatusSuccess,
				values: []uint64{123},
				err:    nil,
			},
		},
		{
			name: "2 parts",
			args: args{
				data: []byte("ok|123;321"),
			},
			want: multiResponseStruct{
				status: ResponseStatusSuccess,
				values: []uint64{123, 321},
				err:    nil,
			},
		},
		{
			name: "3 parts",
			args: args{
				data: []byte("ok|123;321;111"),
			},
			want: multiResponseStruct{
				status: ResponseStatusSuccess,
				values: []uint64{123, 321, 111},
				err:    nil,
			},
		},
		{
			name: "error response",
			args: args{
				data: []byte("err|some error"),
			},
			want: multiResponseStruct{
				status: ResponseStatusError,
				values: nil,
				err:    errors.New("some error"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMultiResponse(tt.args.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}
