package pool

import "testing"

func BenchmarkPoolAcquireReleaseAuditOff(b *testing.B) {
	p, err := New(Config{AuditMode: AuditOff})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, acquireErr := p.Acquire(256)
		if acquireErr != nil {
			b.Fatal(acquireErr)
		}
		if releaseErr := p.Release(lease); releaseErr != nil {
			b.Fatal(releaseErr)
		}
	}
}

func BenchmarkPoolAcquireReleaseAuditFull(b *testing.B) {
	p, err := New(Config{AuditMode: AuditFull})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, acquireErr := p.Acquire(256)
		if acquireErr != nil {
			b.Fatal(acquireErr)
		}
		if releaseErr := p.Release(lease); releaseErr != nil {
			b.Fatal(releaseErr)
		}
	}
}
