#ifndef _RLITE_SCRIPTING_H
#define _RLITE_SCRIPTING_H

#include "hirlite.h"

void evalCommand(struct rliteClient *c);
void evalShaCommand(struct rliteClient *c);
void scriptCommand(rliteClient *c);

#endif
