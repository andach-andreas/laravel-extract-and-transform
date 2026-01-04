<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Data\RemoteSchema;

final class SchemaHasher
{
    public function remoteSchemaHash(RemoteSchema $schema): string
    {
        $payload = array_map(fn ($f) => [
            'name' => $f->name,
            'remoteType' => $f->remoteType,
            'nullable' => $f->nullable,
        ], $schema->fields);

        return sha1(json_encode($payload));
    }

    public function mappingHash(array $mapping): string
    {
        // mapping = list of [remote_name, local_name, local_type, nullable, selected, position]
        return sha1(json_encode($mapping));
    }
}
