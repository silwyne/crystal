module github.com/crystal/kafka

go 1.23.10

require (
    github.com/crystal v0.0.1 
)

replace github.com/crystal => ..


require (
    github.com/twmb/franz-go v1.19.5

	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.11.2 // indirect
)
