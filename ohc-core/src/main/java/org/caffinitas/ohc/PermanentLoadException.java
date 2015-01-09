/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.caffinitas.ohc;

public class PermanentLoadException extends Exception
{
    public PermanentLoadException()
    {
    }

    public PermanentLoadException(Throwable cause)
    {
        super(cause);
    }

    public PermanentLoadException(String message)
    {
        super(message);
    }

    public PermanentLoadException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
