/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.apikeys.persistance.dao;

import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.validators.KeyExpirationValidator;
import com.nvidia.apikeys.vo.KeyByOwnerAndAccountAndServiceVo;
import com.nvidia.apikeys.vo.KeyByOwnerAndServiceVo;
import com.nvidia.boot.jwt.services.mapping.EncryptedModelConverter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KeyByOwnerAndAccountAndServiceModelConverter {

    private final EncryptedModelConverter<KeyByOwnerAndAccountAndServiceModel,
            KeyByOwnerAndAccountAndServiceVo> converter;

    private final KeyExpirationValidator expirationValidator;

    public KeyByOwnerAndAccountAndServiceModel voToModel(KeyByOwnerAndAccountAndServiceVo vo) {
        return converter.voToModel(vo);
    }

    public KeyByOwnerAndServiceVo modelToVo(KeyByOwnerAndAccountAndServiceModel model) {
        var key = converter.modelToVo(model);
        return expirationValidator.validateStatus(KeyByOwnerAndServiceVo.builder()
                .ncaId(key.getNcaId())
                .ncaIdInDetails(key.getNcaIdInDetails())
                .ownerType(key.getOwnerType())
                .ownerId(key.getOwnerId())
                .issuerServiceId(key.getIssuerServiceId())
                .keyId(key.getKeyId())
                .keyStatus(key.getKeyStatus())
                .createdAt(key.getCreatedAt())
                .expiresAt(key.getExpiresAt())
                .deletesAt(key.getDeletesAt())
                .keyHash(key.getKeyHash())
                .apiKeySuffix(key.getApiKeySuffix())
                .description(key.getDescription())
                .audienceServiceIds(key.getAudienceServiceIds())
                .build());
    }
}
