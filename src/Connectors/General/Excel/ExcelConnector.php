<?php

namespace Andach\ExtractAndTransform\Connectors\General\Excel;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Connectors\Contracts\CanInferSchema;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRows;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\Storage;
use OpenSpout\Reader\XLSX\Reader;

class ExcelConnector extends BaseConnector implements CanInferSchema, CanStreamRows
{
    public function key(): string
    {
        return 'excel';
    }

    public function label(): string
    {
        return 'Excel (XLSX)';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'path', label: 'File Path', required: true),
            new ConnectorConfigDefinition(key: 'disk', label: 'Storage Disk', required: false),
        ];
    }

    public function test(array $config): void
    {
        // getPath now performs the existence check
        $this->getPath($config);
    }

    public function datasets(array $config): iterable
    {
        $path = $this->getPath($config);
        $reader = new Reader;
        $reader->open($path);

        foreach ($reader->getSheetIterator() as $sheet) {
            yield new RemoteDataset(
                identifier: $sheet->getName(),
                label: $sheet->getName(),
                meta: ['sheet_index' => $sheet->getIndex()]
            );
        }

        $reader->close();
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        $path = $this->getPath($config);
        $reader = new Reader;
        $reader->open($path);

        $header = [];
        $firstRow = [];

        foreach ($reader->getSheetIterator() as $sheet) {
            if ($sheet->getName() === $dataset->identifier) {
                foreach ($sheet->getRowIterator() as $row) {
                    $cells = $row->toArray();
                    if (empty($header)) {
                        $header = $cells;
                    } else {
                        $firstRow = $cells;
                        break; // We have header and first row
                    }
                }
                break;
            }
        }
        $reader->close();

        if (empty($header)) {
            throw new \RuntimeException("Sheet '{$dataset->identifier}' is empty.");
        }

        $fields = [];
        foreach ($header as $index => $colName) {
            $val = $firstRow[$index] ?? null;
            $fields[] = new RemoteField(
                name: (string) $colName,
                remoteType: gettype($val),
                suggestedLocalType: $this->guessType($val),
                nullable: true
            );
        }

        return new RemoteSchema($fields);
    }

    public function streamRows(RemoteDataset $dataset, array $config): iterable
    {
        $path = $this->getPath($config);
        $reader = new Reader;
        $reader->open($path);

        foreach ($reader->getSheetIterator() as $sheet) {
            if ($sheet->getName() === $dataset->identifier) {
                $header = [];
                foreach ($sheet->getRowIterator() as $row) {
                    $cells = $row->toArray();
                    if (empty($header)) {
                        $header = array_map('strval', $cells);

                        continue;
                    }

                    // Combine header with values
                    $assoc = [];
                    foreach ($header as $i => $key) {
                        $assoc[$key] = $cells[$i] ?? null;
                    }
                    yield $assoc;
                }
                break;
            }
        }

        $reader->close();
    }

    private function getPath(array $config): string
    {
        $path = $config['path'] ?? null;

        if (empty($path)) {
            throw new \RuntimeException("Configuration is missing the required 'path' key.");
        }

        $disk = $config['disk'] ?? null;

        if ($disk) {
            if (! Storage::disk($disk)->exists($path)) {
                throw new \RuntimeException("File not found on disk '{$disk}': {$path}");
            }

            return Storage::disk($disk)->path($path);
        }

        if (! file_exists($path)) {
            throw new \RuntimeException("File not found at path: {$path}");
        }

        return $path;
    }

    private function guessType(mixed $val): string
    {
        if (is_int($val)) {
            return 'integer';
        }
        if (is_float($val)) {
            return 'decimal:18,4';
        }
        if ($val instanceof \DateTimeInterface) {
            return 'datetime';
        }

        return 'string';
    }
}
