<?php

namespace Andach\ExtractAndTransform\Connectors\General\Csv;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\Storage;
use League\Csv\Reader;

class CsvConnector extends BaseConnector
{
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
            new ConnectorConfigDefinition(key: 'path', label: 'File Path', type: 'text', required: true, help: 'Absolute path to the CSV file.'),
            new ConnectorConfigDefinition(key: 'delimiter', label: 'Delimiter', type: 'text', required: false, help: 'Default: ,'),
            new ConnectorConfigDefinition(key: 'disk', label: 'Storage Disk', type: 'text', required: false, help: 'Optional: filesystem disk name (e.g. s3, local)'),
        ];
    }

    public function test(array $config): void
    {
        $path = $config['path'] ?? '';
        $disk = $config['disk'] ?? null;

        if ($disk) {
            if (!Storage::disk($disk)->exists($path)) {
                throw new \RuntimeException("File not found at path: {$path} on disk {$disk}");
            }
        } else {
            if (! file_exists($path)) {
                throw new \RuntimeException("File not found at path: {$path}");
            }
        }
    }

    public function datasets(array $config): array
    {
        $path = $config['path'] ?? '';
        $name = basename($path);

        return [
            new RemoteDataset(identifier: $path, label: $name, meta: []),
        ];
    }

    public function streamRows(RemoteDataset $dataset, array $config, array $options = []): iterable
    {
        $csv = $this->createReader($config);

        foreach ($csv->getRecords() as $record) {
            yield $record;
        }
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        $csv = $this->createReader($config);

        $header = $csv->getHeader();
        $firstRow = $csv->fetchOne(); // Get first row to guess types

        $fields = [];
        foreach ($header as $colName) {
            $value = $firstRow[$colName] ?? null;
            $type = $this->guessType($value);
            $fields[] = new RemoteField(name: $colName, remoteType: 'string', nullable: true, suggestedLocalType: $type);
        }

        return new RemoteSchema($fields);
    }

    private function createReader(array $config): Reader
    {
        $path = $config['path'] ?? '';
        $delimiter = $config['delimiter'] ?? ',';
        $disk = $config['disk'] ?? null;

        try {
            if ($disk) {
                // Ensure file exists or throw specific error
                // We use readStream to get a resource handle
                $stream = Storage::disk($disk)->readStream($path);
                $csv = Reader::createFromStream($stream);
            } else {
                $csv = Reader::createFromPath($path, 'r');
            }
        } catch (\Throwable $e) {
            // Rethrow as RuntimeException with context to match expected behavior
            throw new \RuntimeException("Failed to open stream for CSV file on disk [" . ($disk ?? 'local') . "]: $path", 0, $e);
        }

        $csv->setDelimiter($delimiter);
        $csv->setHeaderOffset(0);

        return $csv;
    }

    private function guessType(?string $value): string
    {
        if ($value === null || $value === '') {
            return 'string';
        }
        if (is_numeric($value)) {
            return str_contains($value, '.') ? 'float' : 'int';
        }
        if (strtotime($value) !== false) {
            return 'datetime';
        }

        return 'string';
    }
}
