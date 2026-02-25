<?php

namespace Andach\ExtractAndTransform\Connectors\General\Excel;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use OpenSpout\Reader\Common\Creator\ReaderEntityFactory;

class ExcelConnector extends BaseConnector
{
    public function key(): string
    {
        return 'excel';
    }

    public function label(): string
    {
        return 'Excel (XLSX/ODS)';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'path', label: 'File Path', type: 'text', required: true, help: 'Absolute path to the Excel file.'),
        ];
    }

    public function test(array $config): void
    {
        $path = $config['path'] ?? '';
        if (! file_exists($path)) {
            throw new \RuntimeException("File not found at path: {$path}");
        }
    }

    public function datasets(array $config): array
    {
        $path = $config['path'] ?? '';
        $reader = ReaderEntityFactory::createReaderFromFile($path);
        $reader->open($path);

        $datasets = [];
        foreach ($reader->getSheetIterator() as $sheet) {
            $datasets[] = new RemoteDataset(identifier: $sheet->getName(), label: $sheet->getName(), meta: ['index' => $sheet->getIndex()]);
        }
        $reader->close();

        return $datasets;
    }

    public function streamRows(RemoteDataset $dataset, array $config, array $options = []): iterable
    {
        $path = $config['path'] ?? '';
        $reader = ReaderEntityFactory::createReaderFromFile($path);
        $reader->open($path);

        $sheetIndex = $dataset->meta['index'] ?? 0;
        $header = [];

        foreach ($reader->getSheetIterator() as $sheet) {
            if ($sheet->getIndex() !== $sheetIndex) {
                continue;
            }

            foreach ($sheet->getRowIterator() as $rowIndex => $row) {
                $cells = $row->toArray();
                if ($rowIndex === 1) {
                    $header = $cells;
                    continue;
                }

                $assocRow = [];
                foreach ($header as $i => $colName) {
                    $assocRow[$colName] = $cells[$i] ?? null;
                }
                yield $assocRow;
            }
        }
        $reader->close();
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        $path = $config['path'] ?? '';
        $reader = ReaderEntityFactory::createReaderFromFile($path);
        $reader->open($path);

        $sheetIndex = $dataset->meta['index'] ?? 0;
        $fields = [];

        foreach ($reader->getSheetIterator() as $sheet) {
            if ($sheet->getIndex() !== $sheetIndex) {
                continue;
            }

            $header = [];
            $firstRow = [];

            foreach ($sheet->getRowIterator() as $rowIndex => $row) {
                if ($rowIndex === 1) {
                    $header = $row->toArray();
                } elseif ($rowIndex === 2) {
                    $firstRow = $row->toArray();
                    break; // Only need first data row
                }
            }

            foreach ($header as $i => $colName) {
                $val = $firstRow[$i] ?? null;
                $type = is_numeric($val) ? (str_contains((string)$val, '.') ? 'float' : 'int') : 'string';
                $fields[] = new RemoteField(name: (string)$colName, remoteType: 'string', nullable: true, suggestedLocalType: $type);
            }
            break;
        }
        $reader->close();

        return new RemoteSchema($fields);
    }
}
