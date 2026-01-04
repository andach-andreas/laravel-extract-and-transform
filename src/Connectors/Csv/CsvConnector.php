<?php

namespace Andach\ExtractAndTransform\Connectors\Csv;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Andach\ExtractAndTransform\Services\RetryService;
use Illuminate\Support\Facades\Storage;
use InvalidArgumentException;
use RuntimeException;

final class CsvConnector extends BaseConnector
{
    private RetryService $retryService;

    public function __construct()
    {
        $this->retryService = app(RetryService::class);
    }

    public function key(): string
    {
        return 'csv';
    }

    public function label(): string
    {
        return 'CSV';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'path', label: 'File Path', type: 'text', required: true, help: 'Absolute path to the CSV file on the server.'),
            new ConnectorConfigDefinition(key: 'disk', label: 'Storage Disk', type: 'text', required: false, help: 'The name of the filesystem disk (e.g., "s3", "local"). If omitted, path is treated as a local absolute path.'),
        ];
    }

    public function test(array $config): void
    {
        $this->retryService->run(function () use ($config) {
            $path = $this->pathFromConfig($config);
            $disk = $config['disk'] ?? null;

            if ($disk) {
                if (! Storage::disk($disk)->exists($path)) {
                    throw new RuntimeException("CSV file does not exist on disk [{$disk}]: {$path}");
                }
                // Try opening a stream to ensure readability
                $stream = Storage::disk($disk)->readStream($path);
                if ($stream === false || $stream === null) {
                    throw new RuntimeException("Failed to open stream for CSV file on disk [{$disk}]: {$path}");
                }
                fclose($stream);
            } else {
                if (! file_exists($path)) {
                    throw new RuntimeException("CSV file does not exist: {$path}");
                }
                if (! is_readable($path)) {
                    throw new RuntimeException("CSV file is not readable: {$path}");
                }
                $fh = @fopen($path, 'rb');
                if ($fh === false) {
                    throw new RuntimeException("Failed to open CSV file: {$path}");
                }
                fclose($fh);
            }
        });
    }

    public function datasets(array $config): iterable
    {
        $path = $this->pathFromConfig($config);
        yield new RemoteDataset(identifier: $path, label: basename($path), meta: ['path' => $path]);
    }

    public function streamRows(RemoteDataset $dataset, array $config): iterable
    {
        $path = $dataset->meta['path'] ?? $dataset->identifier;
        $disk = $config['disk'] ?? null;

        $fh = $this->retryService->run(function () use ($path, $disk) {
            if ($disk) {
                $stream = Storage::disk($disk)->readStream($path);
                if ($stream === false || $stream === null) {
                    throw new RuntimeException("Failed to open stream for CSV file on disk [{$disk}]: {$path}");
                }

                return $stream;
            }

            return @fopen($path, 'rb') ?: throw new RuntimeException("Failed to open CSV file: {$path}");
        });

        try {
            $header = fgetcsv($fh);
            if (! is_array($header) || count($header) === 0) {
                throw new RuntimeException("CSV has no header row: {$path}");
            }
            $header = array_map(fn ($h) => trim((string) $h), $header);
            while (($row = fgetcsv($fh)) !== false) {
                $assoc = [];
                foreach ($header as $i => $name) {
                    $assoc[$name] = $row[$i] ?? null;
                }
                yield $assoc;
            }
        } finally {
            fclose($fh);
        }
    }

    private function pathFromConfig(array $config): string
    {
        $path = $config['path'] ?? null;
        if (! is_string($path) || trim($path) === '') {
            throw new InvalidArgumentException('CSV connector requires config key [path].');
        }

        return $path;
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        $path = $dataset->meta['path'] ?? $dataset->identifier;
        $disk = $config['disk'] ?? null;

        if ($disk) {
            $fh = Storage::disk($disk)->readStream($path);
            if ($fh === false || $fh === null) {
                throw new RuntimeException("Failed to open stream for CSV file on disk [{$disk}]: {$path}");
            }
        } else {
            $fh = fopen($path, 'rb');
            if ($fh === false) {
                throw new RuntimeException("Failed to open CSV file: {$path}");
            }
        }

        $header = fgetcsv($fh);
        if (! is_array($header) || count($header) === 0) {
            fclose($fh);
            throw new RuntimeException("CSV has no header row: {$path}");
        }
        $header = array_map(fn ($h) => trim((string) $h), $header);
        $samples = [];
        $max = 50;
        $i = 0;
        while (($row = fgetcsv($fh)) !== false && $i < $max) {
            $i++;
            foreach ($header as $idx => $name) {
                $samples[$name][] = $row[$idx] ?? null;
            }
        }
        fclose($fh);
        $fields = [];
        foreach ($header as $pos => $name) {
            $vals = $samples[$name] ?? [];
            [$suggested, $nullable] = $this->guessLocalTypeAndNullability($vals);
            $fields[] = new RemoteField(name: $name, remoteType: 'csv', nullable: $nullable, suggestedLocalType: $suggested);
        }

        return new RemoteSchema($fields);
    }

    private function guessLocalTypeAndNullability(array $values): array
    {
        $nullable = false;
        $seen = array_filter($values, function ($v) use (&$nullable) {
            if ($v === null) {
                $nullable = true;

                return false;
            }
            $s = trim((string) $v);
            if ($s === '') {
                $nullable = true;

                return false;
            }

            return true;
        });
        if (count($seen) === 0) {
            return ['string', true];
        }
        $allInt = true;
        $allFloat = true;
        $allBool = true;
        $allDate = true;
        $allDateTime = true;
        foreach ($seen as $v) {
            $s = trim((string) $v);
            if (! preg_match('/^-?\d+$/', $s)) {
                $allInt = false;
            }
            if (! is_numeric($s)) {
                $allFloat = false;
            }
            $ls = strtolower($s);
            if (! in_array($ls, ['0', '1', 'true', 'false', 'yes', 'no', 'y', 'n'], true)) {
                $allBool = false;
            }
            if (strtotime($s) === false) {
                $allDate = false;
                $allDateTime = false;
            } else {
                if (preg_match('/\d:\d/', $s) || str_contains($s, 'T')) {
                    $allDate = false;
                } else {
                    $allDateTime = false;
                }
            }
        }
        if ($allInt) {
            return ['int', $nullable];
        }
        if ($allBool) {
            return ['bool', $nullable];
        }
        if ($allDate) {
            return ['date', $nullable];
        }
        if ($allDateTime) {
            return ['datetime', $nullable];
        }
        if ($allFloat) {
            return ['float', $nullable];
        }

        return ['string', $nullable];
    }
}
