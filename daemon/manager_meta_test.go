package daemon

import "testing"

func TestMetaFromConfigPreservesInstallSettings(t *testing.T) {
	cfg := Config{
		LogFile:       `C:\logs\cc-connect.log`,
		LogMaxSize:    42,
		LogMaxBackups: 7,
		WorkDir:       `C:\work`,
		BinaryPath:    `C:\bin\cc-connect.exe`,
	}

	meta := MetaFromConfig(cfg)
	if meta.LogFile != cfg.LogFile {
		t.Fatalf("LogFile = %q, want %q", meta.LogFile, cfg.LogFile)
	}
	if meta.LogMaxSize != cfg.LogMaxSize {
		t.Fatalf("LogMaxSize = %d, want %d", meta.LogMaxSize, cfg.LogMaxSize)
	}
	if meta.LogMaxBackups != cfg.LogMaxBackups {
		t.Fatalf("LogMaxBackups = %d, want %d", meta.LogMaxBackups, cfg.LogMaxBackups)
	}
	if meta.WorkDir != cfg.WorkDir {
		t.Fatalf("WorkDir = %q, want %q", meta.WorkDir, cfg.WorkDir)
	}
	if meta.BinaryPath != cfg.BinaryPath {
		t.Fatalf("BinaryPath = %q, want %q", meta.BinaryPath, cfg.BinaryPath)
	}
	if meta.InstalledAt == "" {
		t.Fatal("InstalledAt is empty")
	}
}
