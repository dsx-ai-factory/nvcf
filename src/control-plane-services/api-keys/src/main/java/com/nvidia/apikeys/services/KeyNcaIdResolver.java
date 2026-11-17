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

package com.nvidia.apikeys.services;

import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.vo.KeyVo;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KeyNcaIdResolver {

    private final NakProperties nakProperties;

    // Column value, else key details, else the configured NCA ID.
    public String resolve(KeyVo key) {
        return resolve(key.getNcaId(), key.getNcaIdInDetails());
    }

    public String resolve(String ncaIdColumn, String ncaIdInDetails) {
        var column = StringUtils.trimToNull(ncaIdColumn);
        var details = StringUtils.trimToNull(ncaIdInDetails);
        if (StringUtils.isNotBlank(column) && StringUtils.isNotBlank(details)
                && !column.equals(details)) {
            throw new IllegalStateException(
                    "NCA ID value in the column does not match with the corresponding value in the details");
        }
        if (StringUtils.isNotBlank(column)) {
            return column;
        }
        return StringUtils.isNotBlank(details) ? details : nakProperties.getNcaId();
    }
}
